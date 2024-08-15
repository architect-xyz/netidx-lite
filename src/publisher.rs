use crate::try_continue;
use anyhow::Result;
use log::{debug, error};
use std::time::Duration;
use tokio::net::TcpListener;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(bind_to: &str) -> Result<()> {
    let listener = TcpListener::bind(bind_to).await?;
    loop {
        match listener.accept().await {
            Err(e) => error!("failed to accept connection: {e:?}"),
            Ok((s, addr)) => {
                debug!("accepted client: {addr:?}");
                try_continue!("set nodelay", s.set_nodelay(true));
            }
        }
    }
    #[allow(unreachable_code)]
    Ok(())
}

pub async fn run_client() -> Result<()> {
    let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
    Ok(())
}
