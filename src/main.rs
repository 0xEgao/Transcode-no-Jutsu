use aws_sdk_sqs::Client as SqsClient;
use std::env;
use std::process::Command;

mod types;
use types::S3Event;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_config::load_from_env().await;
    let sqs_client = SqsClient::new(&config);

    let queue_url = "https://sqs.us-east-1.amazonaws.com/607696765426/video-pipeline-queue-0306";

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

                                let aws_access_key = env::var("AWS_ACCESS_KEY_ID")
                                    .expect("AWS_ACCESS_KEY_ID not set");
                                let aws_secret_key = env::var("AWS_SECRET_ACCESS_KEY")
                                    .expect("AWS_SECRET_ACCESS_KEY not set");
                                let aws_region = env::var("AWS_REGION")
                                    .unwrap_or_else(|_| "us-east-1".to_string());
                                let aws_session_token =
                                    env::var("AWS_SESSION_TOKEN").unwrap_or_default();

                                let mut args: Vec<String> = vec![
                                    "run".to_string(),
                                    "-e".to_string(),
                                    format!("AWS_ACCESS_KEY_ID={}", aws_access_key),
                                    "-e".to_string(),
                                    format!("AWS_SECRET_ACCESS_KEY={}", aws_secret_key),
                                    "-e".to_string(),
                                    format!("AWS_REGION={}", aws_region),
                                    "-e".to_string(),
                                    format!("SOURCE_KEY={}", key),
                                ];

                                if !aws_session_token.is_empty() {
                                    args.push("-e".to_string());
                                    args.push(format!("AWS_SESSION_TOKEN={}", aws_session_token));
                                }

                                args.push("video-transcoder".to_string());

                                println!("Launching video-transcoder container for key {}", key);
                                let status = Command::new("docker").args(&args).status()?;
                                if !status.success() {
                                    eprintln!("Error: docker run failed ({:?})", status);
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
