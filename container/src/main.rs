use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;

const SOURCE_BUCKET: &str = "temp-video-storage-0342";
const DEST_BUCKET: &str = "perm-video-storage-0342";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source_key = env::var("SOURCE_KEY").expect("SOURCE_KEY environment variable not set");

    println!("Starting transcoding job");
    println!("Source: s3://{}/{}", SOURCE_BUCKET, source_key);
    println!("Destination: s3://{}", DEST_BUCKET);

    let config = aws_config::load_from_env().await;
    let s3_client = S3Client::new(&config);

    let input_path = "/tmp/input.mp4";
    println!("Downloading video from S3...");
    download_from_s3(&s3_client, SOURCE_BUCKET, &source_key, input_path).await?;

    let resolutions = vec![
        ("480p", "scale=854:480", "1000k"),
        ("720p", "scale=1280:720", "2500k"),
        ("1080p", "scale=1920:1080", "5000k"),
    ];

    for (name, scale, bitrate) in resolutions {
        let output_path = format!("/tmp/output_{}.mp4", name);
        println!("Transcoding to {}...", name);
        transcode_video(input_path, &output_path, scale, bitrate)?;

        let dest_key = format!(
            "{}/{}.mp4",
            Path::new(&source_key)
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap(),
            name
        );

        println!("Uploading {} to s3://{}/{}", name, DEST_BUCKET, dest_key);
        upload_to_s3(&s3_client, DEST_BUCKET, &dest_key, &output_path).await?;

        std::fs::remove_file(&output_path)?;
        println!("Completed {}", name);
    }

    std::fs::remove_file(input_path)?;
    println!("Transcoding job completed successfully");
    Ok(())
}

async fn download_from_s3(
    client: &S3Client,
    bucket: &str,
    key: &str,
    destination: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut object = client.get_object().bucket(bucket).key(key).send().await?;
    let mut file = File::create(destination)?;

    while let Some(bytes) = object.body.try_next().await? {
        file.write_all(&bytes)?;
    }

    println!(
        "Downloaded video from S3 ({} bytes)",
        std::fs::metadata(destination)?.len()
    );
    Ok(())
}

async fn upload_to_s3(
    client: &S3Client,
    bucket: &str,
    key: &str,
    file_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = ByteStream::from_path(Path::new(file_path)).await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .content_type("video/mp4")
        .send()
        .await?;
    Ok(())
}

fn transcode_video(
    input: &str,
    output: &str,
    scale: &str,
    video_bitrate: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("ffmpeg")
        .args(&[
            "-i",
            input,
            "-vf",
            scale,
            "-c:v",
            "libx264",
            "-b:v",
            video_bitrate,
            "-preset",
            "medium",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            "-y",
            output,
        ])
        .status()?;
    if !status.success() {
        return Err(format!("FFmpeg failed with status: {}", status).into());
    }
    Ok(())
}
