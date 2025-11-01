use aws_sdk_ecs::Client as EcsClient;
use aws_sdk_ecs::types::{
    AssignPublicIp, AwsVpcConfiguration, ContainerOverride, KeyValuePair, LaunchType,
    NetworkConfiguration, TaskOverride,
};
use aws_sdk_sqs::Client as SqsClient;

mod types;
use types::S3Event;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_config::load_from_env().await;
    let sqs_client = SqsClient::new(&config);
    let ecs_client = EcsClient::new(&config);

    let queue_url = "https://sqs.us-east-1.amazonaws.com/607696765426/video-pipeline-queue-0306";

    // ECS Configuration
    let cluster_name = "0306";
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
                                let bucket = record.s3.bucket.name;
                                let key = record.s3.object.key;

                                println!("New S3 Upload Event:");
                                println!("Bucket: {}", bucket);
                                println!("Key: {}", key);

                                match run_transcoding_task(
                                    &ecs_client,
                                    cluster_name,
                                    task_definition,
                                    &key,
                                )
                                .await
                                {
                                    Ok(task_arn) => {
                                        println!("Started ECS task: {}", task_arn);

                                        if let Some(receipt) = msg.receipt_handle() {
                                            sqs_client
                                                .delete_message()
                                                .queue_url(queue_url)
                                                .receipt_handle(receipt)
                                                .send()
                                                .await?;
                                            println!("🗑️  Deleted message from queue");
                                        }
                                    }
                                    Err(err) => {
                                        eprintln!("Failed to start ECS task: {:?}", err);
                                    }
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

async fn run_transcoding_task(
    client: &EcsClient,
    cluster: &str,
    task_definition: &str,
    source_key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let vpc_config = AwsVpcConfiguration::builder()
        .subnets("subnet-0f9a913de5cf3e937")
        .subnets("subnet-0b0af94fce4890072")
        .subnets("subnet-079faba8373e26ede")
        .security_groups("sg-06e74305073792022")
        .assign_public_ip(AssignPublicIp::Enabled)
        .build()?;

    let network_config = NetworkConfiguration::builder()
        .awsvpc_configuration(vpc_config)
        .build();

    let env_var = KeyValuePair::builder()
        .name("SOURCE_KEY")
        .value(source_key)
        .build();

    let container_override = ContainerOverride::builder()
        .name("video-transcoder")
        .environment(env_var)
        .build();

    let task_override = TaskOverride::builder()
        .container_overrides(container_override)
        .build();

    let result = client
        .run_task()
        .cluster(cluster)
        .task_definition(task_definition)
        .launch_type(LaunchType::Fargate)
        .network_configuration(network_config)
        .overrides(task_override)
        .count(1)
        .send()
        .await?;

    if let Some(tasks) = result.tasks {
        if let Some(task) = tasks.first() {
            if let Some(arn) = task.task_arn() {
                return Ok(arn.to_string());
            }
        }
    }

    Err("No task ARN returned from ECS".into())
}
