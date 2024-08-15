use crate::try_continue;
use anyhow::Result;
use log::{debug, error};
use tokio::net::TcpListener;

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
