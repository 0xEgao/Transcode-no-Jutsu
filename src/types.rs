use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct S3Event {
    #[serde(rename = "Records")]
    pub records: Vec<S3EventRecord>,
}

#[derive(Debug, Deserialize)]
pub struct S3EventRecord {
    pub s3: S3Entity,
}

#[derive(Debug, Deserialize)]
pub struct S3Entity {
    pub bucket: S3Bucket,
    pub object: S3Object,
}

#[derive(Debug, Deserialize)]
pub struct S3Bucket {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct S3Object {
    pub key: String,
}
