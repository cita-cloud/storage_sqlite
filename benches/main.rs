use cita_cloud_proto::storage::{storage_service_client::StorageServiceClient, Content};
use tonic::Request;

pub async fn store_data(
    storage_port: u16,
    region: u32,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let storage_addr = format!("http://127.0.0.1:{}", storage_port);
    let mut client = StorageServiceClient::connect(storage_addr).await?;

    let request = Request::new(Content { region, key, value });

    let response = client.store(request).await?;
    Ok(response.into_inner().is_success)
}

/*
pub async fn batch_store_data(
    storage_port: u16,
    region: u32,
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let storage_addr = format!("http://127.0.0.1:{}", storage_port);
    let mut client = StorageServiceClient::connect(storage_addr).await?;

    let mut contents = Vec::new();
    for i in 0..keys.len() {
        let key = keys[i].to_owned();
        let value = values[i].to_owned();
        let content = Content { region, key, value };
        contents.push(content);
    }

    let request = Request::new(ContentArray { contents });

    let response = client.batch_store(request).await?;
    Ok(response.into_inner().is_success)
}
*/

fn main() {
    let _ = run();
}
/*
#[tokio::main]
async fn batch_run() -> Result<(), Box<dyn std::error::Error>> {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for i in 0..1000u64 {
        let mut hash = Vec::new();
        for _ in 0..4 {
            hash.extend_from_slice(&i.to_be_bytes().to_vec());
        }
        keys.push(hash);
        values.push(i.to_be_bytes().to_vec());
    }
    batch_store_data(
        50003,
        9,
        keys,
        values,
    ).await.map(|_| ())
}
*/

#[tokio::main]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..1000u64 {
        let mut hash = Vec::new();
        for _ in 0..4 {
            hash.extend_from_slice(&i.to_be_bytes().to_vec());
        }
        let _ = store_data(
            50003,
            9,
            hash.to_vec(),
            i.to_be_bytes().to_vec(),
        ).await?;
    }
    Ok(())
}
