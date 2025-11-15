use actix_web::{App, HttpServer, web};
use aws_sdk_s3::client;

use crate::upload::upload_video;

mod upload;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let config = aws_config::load_from_env().await;
    let s3_client = client::Client::new(&config);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(s3_client.clone()))
            .service(upload_video)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
