use std::sync::{Arc, Mutex};
use std::{env, io, time::Duration};

use aws_sdk_ecs::Client as EcsClient;
use aws_sdk_ecs::types::{
    AssignPublicIp, AwsVpcConfiguration, ContainerOverride, KeyValuePair, LaunchType,
    NetworkConfiguration, TaskOverride,
};
use aws_sdk_sqs::Client as SqsClient;

use crossterm::event::{self, Event as CEvent, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};

use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

use tokio::time::sleep;

mod types;
use types::S3Event;

#[derive(Clone, Debug)]
struct VideoMessage {
    bucket: String,
    key: String,
    receipt_handle: String,
}

#[derive(Debug)]
struct AppState {
    messages: Vec<VideoMessage>,
    selected: usize,
}

impl AppState {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
            selected: 0,
        }
    }

    fn push_message(&mut self, m: VideoMessage) {
        self.messages.push(m);
        if self.messages.len() == 1 {
            self.selected = 0;
        }
    }

    fn remove_selected(&mut self) -> Option<VideoMessage> {
        if self.messages.is_empty() {
            return None;
        }
        if self.selected >= self.messages.len() {
            self.selected = self.messages.len() - 1;
        }
        Some(self.messages.remove(self.selected))
    }

    fn next(&mut self) {
        if !self.messages.is_empty() {
            self.selected = (self.selected + 1).min(self.messages.len() - 1);
        }
    }

    fn previous(&mut self) {
        if !self.messages.is_empty() {
            if self.selected == 0 {
                self.selected = 0;
            } else {
                self.selected -= 1;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_config::load_from_env().await;
    let sqs_client = SqsClient::new(&config);
    let ecs_client = EcsClient::new(&config);

    let queue_url = "https://sqs.us-east-1.amazonaws.com/091049244748/video-pipeline-queue-0342";
    let cluster_name = "0342-video";
    let task_definition = "video-transcoder:1";

    let state = Arc::new(Mutex::new(AppState::new()));

    let sqs_for_poller = sqs_client.clone();
    let queue_for_poller = queue_url.to_string();
    let state_for_poller = Arc::clone(&state);

    tokio::spawn(async move {
        loop {
            let resp = sqs_for_poller
                .receive_message()
                .queue_url(&queue_for_poller)
                .max_number_of_messages(10)
                .wait_time_seconds(4)
                .send()
                .await;

            match resp {
                Ok(output) => {
                    if let Some(messages) = output.messages {
                        for msg in messages {
                            if let (Some(body), Some(receipt)) = (msg.body(), msg.receipt_handle())
                            {
                                match serde_json::from_str::<S3Event>(body) {
                                    Ok(event) => {
                                        for rec in event.records {
                                            let v = VideoMessage {
                                                bucket: rec.s3.bucket.name,
                                                key: rec.s3.object.key,
                                                receipt_handle: receipt.to_string(),
                                            };
                                            if let Ok(mut st) = state_for_poller.lock() {
                                                st.push_message(v.clone());
                                                eprintln!(
                                                    "SQS poller: added job {} / {} (total {})",
                                                    v.bucket,
                                                    v.key,
                                                    st.messages.len()
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "Failed to parse S3 event from SQS body: {:?}. body: {}",
                                            e, body
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("SQS receive_message error: {:?}", e);
                }
            }

            sleep(Duration::from_secs(5)).await;
        }
    });

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(
        &mut terminal,
        state,
        sqs_client,
        ecs_client,
        queue_url.to_string(),
        cluster_name.to_string(),
        task_definition.to_string(),
    )
    .await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: Arc<Mutex<AppState>>,
    sqs_client: SqsClient,
    ecs_client: EcsClient,
    queue_url: String,
    cluster_name: String,
    task_definition: String,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let (items_snapshot, selected_index) = {
            let st = state.lock().unwrap();
            (st.messages.clone(), st.selected)
        };

        terminal.draw(|f| {
            let size = f.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints(
                    [
                        Constraint::Length(4),
                        Constraint::Min(6),
                        Constraint::Length(3),
                    ]
                    .as_ref(),
                )
                .split(size);

            let header = Paragraph::new(vec![
                Line::from(Span::styled(
                    "Transcode no Jutsu",
                    Style::default()
                        .fg(Color::LightCyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(Span::raw(
                    "A highly efficient video transcoding library built in Rust using FFmpeg",
                )),
            ])
            .block(Block::default().borders(Borders::ALL).title("About"));
            f.render_widget(header, chunks[0]);

            if items_snapshot.is_empty() {
                let empty = Paragraph::new("No video upload events yet...").block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Pending Uploads"),
                );
                f.render_widget(empty, chunks[1]);
            } else {
                let list_items: Vec<ListItem> = items_snapshot
                    .iter()
                    .map(|m| {
                        let line = format!("{} / {}", m.bucket, m.key);
                        ListItem::new(Span::raw(line))
                    })
                    .collect();

                let mut list_state = ratatui::widgets::ListState::default();
                list_state.select(Some(selected_index));

                let list = List::new(list_items)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Pending Uploads"),
                    )
                    .highlight_style(
                        Style::default()
                            .bg(Color::Blue)
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    )
                    .highlight_symbol(">> ");

                f.render_stateful_widget(list, chunks[1], &mut list_state);
            }

            let help =
                Paragraph::new("Controls: ↑/↓ to move • Enter to transcode selected • q to quit")
                    .block(Block::default().borders(Borders::ALL).title("Help"));
            f.render_widget(help, chunks[2]);
        })?;

        tokio::task::yield_now().await;

        if event::poll(Duration::from_millis(150))? {
            if let CEvent::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Down => {
                        let mut st = state.lock().unwrap();
                        st.next();
                    }
                    KeyCode::Up => {
                        let mut st = state.lock().unwrap();
                        st.previous();
                    }
                    KeyCode::Enter => {
                        let maybe_job = {
                            let mut st = state.lock().unwrap();
                            st.remove_selected()
                        };
                        if let Some(job) = maybe_job {
                            let ecs_for_task = ecs_client.clone();
                            let sqs_for_task = sqs_client.clone();
                            let queue_for_task = queue_url.clone();
                            let cluster_for_task = cluster_name.clone();
                            let task_def_for_task = task_definition.clone();

                            tokio::spawn(async move {
                                if let Err(e) = run_and_delete(
                                    job,
                                    ecs_for_task,
                                    sqs_for_task,
                                    queue_for_task,
                                    cluster_for_task,
                                    task_def_for_task,
                                )
                                .await
                                {
                                    eprintln!("Error running ECS task: {:?}", e);
                                }
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
        sleep(Duration::from_millis(20)).await;
    }

    Ok(())
}

async fn run_and_delete(
    job: VideoMessage,
    ecs_client: EcsClient,
    sqs_client: SqsClient,
    queue_url: String,
    cluster_name: String,
    task_definition: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    eprintln!("Starting ECS task for key: {}", job.key);

    let aws_access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
    let aws_secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
    let aws_region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let aws_session_token = env::var("AWS_SESSION_TOKEN").unwrap_or_default();

    let mut env_vars = vec![
        KeyValuePair::builder()
            .name("SOURCE_KEY")
            .value(job.key.clone())
            .build(),
        KeyValuePair::builder()
            .name("AWS_ACCESS_KEY_ID")
            .value(aws_access_key)
            .build(),
        KeyValuePair::builder()
            .name("AWS_SECRET_ACCESS_KEY")
            .value(aws_secret_key)
            .build(),
        KeyValuePair::builder()
            .name("AWS_REGION")
            .value(aws_region)
            .build(),
    ];
    if !aws_session_token.is_empty() {
        env_vars.push(
            KeyValuePair::builder()
                .name("AWS_SESSION_TOKEN")
                .value(aws_session_token)
                .build(),
        );
    }

    let vpc_config = AwsVpcConfiguration::builder()
        .subnets("subnet-0e00f1da12d6bc546")
        .subnets("subnet-0bfd5432d208ee8ff")
        .subnets("subnet-069cb61c8663259a3")
        .security_groups("sg-0d946d4f1351b5bfd")
        .assign_public_ip(AssignPublicIp::Enabled)
        .build()?;

    let network_config = NetworkConfiguration::builder()
        .awsvpc_configuration(vpc_config)
        .build();

    let container_override = ContainerOverride::builder()
        .name("video-transcoder")
        .set_environment(Some(env_vars))
        .build();

    let task_override = TaskOverride::builder()
        .set_container_overrides(Some(vec![container_override]))
        .build();

    let run_resp = ecs_client
        .run_task()
        .cluster(cluster_name)
        .task_definition(task_definition)
        .launch_type(LaunchType::Fargate)
        .network_configuration(network_config)
        .overrides(task_override)
        .count(1)
        .send()
        .await;

    match run_resp {
        Ok(out) => {
            if let Some(tasks) = out.tasks {
                if let Some(t) = tasks.get(0) {
                    eprintln!("ECS started: {:?}", t.task_arn());
                }
            } else if let Some(failures) = out.failures {
                eprintln!("ECS failures: {:?}", failures);
            } else {
                eprintln!("ECS run_task returned neither tasks nor failures.");
            }
        }
        Err(e) => {
            eprintln!("ECS run_task error: {:?}", e);
            return Err(Box::new(e));
        }
    }

    if !job.receipt_handle.is_empty() {
        match sqs_client
            .delete_message()
            .queue_url(&queue_url)
            .receipt_handle(job.receipt_handle.clone())
            .send()
            .await
        {
            Ok(_) => eprintln!("Deleted SQS message for key {}", job.key),
            Err(e) => eprintln!("Failed to delete SQS message: {:?}", e),
        }
    }

    Ok(())
}
