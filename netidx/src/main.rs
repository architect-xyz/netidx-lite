use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    println!("Hello, world!");
    Ok(())
}
