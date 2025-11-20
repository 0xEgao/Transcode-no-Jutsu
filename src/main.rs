use aws_sdk_ecs::Client as EcsClient;
use aws_sdk_ecs::types::{
    AssignPublicIp, AwsVpcConfiguration, ContainerOverride, KeyValuePair, LaunchType,
    NetworkConfiguration, TaskOverride,
};
use aws_sdk_sqs::Client as SqsClient;
use std::env;

mod types;
use types::S3Event;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_config::load_from_env().await;
    let sqs_client = SqsClient::new(&config);
    let ecs_client = EcsClient::new(&config);

    let queue_url = "https://sqs.us-east-1.amazonaws.com/091049244748/video-pipeline-queue-0342";
    let cluster_name = "0342-video";
    let task_definition = "video-transcoder:1";

    println!("Listening for S3 events on SQS...");

    loop {
        let resp = sqs_client
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(5)
            .wait_time_seconds(10)
            .send()
            .await?;

        if let Some(messages) = resp.messages {
            for msg in messages {
                if let Some(body) = msg.body() {
                    match serde_json::from_str::<S3Event>(body) {
                        Ok(event) => {
                            for record in event.records {
                                let bucket = &record.s3.bucket.name;
                                let key = &record.s3.object.key;
                                println!("New S3 Upload Event:");
                                println!("Bucket: {}", bucket);
                                println!("Key: {}", key);

                                // Read AWS env vars from local env; fallback to blank if missing.
                                let aws_access_key =
                                    env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
                                let aws_secret_key =
                                    env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
                                let aws_region = env::var("AWS_REGION")
                                    .unwrap_or_else(|_| "us-east-1".to_string());
                                let aws_session_token =
                                    env::var("AWS_SESSION_TOKEN").unwrap_or_default();

                                let mut env_vars = vec![
                                    KeyValuePair::builder()
                                        .name("SOURCE_KEY")
                                        .value(key)
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
                                    .container_overrides(container_override)
                                    .build();

                                let result = ecs_client
                                    .run_task()
                                    .cluster(cluster_name)
                                    .task_definition(task_definition)
                                    .launch_type(LaunchType::Fargate)
                                    .network_configuration(network_config)
                                    .overrides(task_override)
                                    .count(1)
                                    .send()
                                    .await?;

                                if let Some(tasks) = result.tasks {
                                    if let Some(task) = tasks.get(0) {
                                        if let Some(task_arn) = task.task_arn() {
                                            println!("Started ECS task: {}", task_arn);
                                        }
                                    }
                                }

                                if let Some(receipt) = msg.receipt_handle() {
                                    sqs_client
                                        .delete_message()
                                        .queue_url(queue_url)
                                        .receipt_handle(receipt)
                                        .send()
                                        .await?;
                                    println!("Deleted message from queue");
                                }
                            }
                        }
                        Err(err) => {
                            eprintln!("Failed to parse S3 event: {:?}", err);
                            eprintln!("Raw body: {}", body);
                        }
                    }
                }
            }
        }
    }
}
