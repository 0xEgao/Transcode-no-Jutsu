use actix_multipart::Multipart;
use actix_web::{Error, HttpResponse, post, web};
use aws_sdk_s3::Client as S3Client;
use bytes::BytesMut;
use futures_util::StreamExt;

#[post("/upload")]
async fn upload_video(
    mut payload: Multipart,
    s3: web::Data<S3Client>,
) -> Result<HttpResponse, Error> {
    // For very large videos, stream directly → S3 multipart upload.
    let mut video_bytes = BytesMut::new();

    while let Some(item) = payload.next().await {
        let mut field = item?;
        let content_disposition = field.content_disposition().unwrap();

        if content_disposition.get_name() == Some("video") {
            while let Some(chunk) = field.next().await {
                let data = chunk?;
                video_bytes.extend_from_slice(&data);
            }
        }
    }

    let file_name = format!("upload-{}.mp4", uuid::Uuid::new_v4());

    s3.put_object()
        .bucket("temp-video-storage-0306")
        .key(&file_name)
        .body(video_bytes.freeze().into())
        .send()
        .await
        .map_err(|e| {
            println!("S3 error: {:?}", e);
            actix_web::error::ErrorInternalServerError("Upload failed")
        })?;

    Ok(HttpResponse::Ok().body(format!("Uploaded as {}", file_name)))
}
