use tokio;
use http_datafusion::sample;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    sample::run().await?;
    Ok(())
}
