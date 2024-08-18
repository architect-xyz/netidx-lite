use anyhow::{bail, Result};
use netidx_lite::{
    protocol::{AuthMethod, ClientHello, ToPublisher, ToSubscriber, MAGIC},
    publisher::Publisher,
    socket_channel,
    socket_channel::Channel,
};
use std::{future, time::Duration};
use tokio::{net::TcpStream, task, time};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    task::spawn(async move { publisher().await });
    task::spawn(async move { subscriber().await });
    future::pending::<()>().await;
    Ok(())
}

async fn publisher() -> Result<()> {
    println!("pub | starting...");
    let publisher = Publisher::new("127.0.0.1:7777".parse()?, 5).await?;
    println!("pub | ready");
    let val = publisher.publish("/hello".into(), String::from("world"))?;
    println!("pub | all published");
    const OPTIONS: [&'static str; 3] = ["world", "moon", "sun"];
    for i in 0.. {
        time::sleep(Duration::from_secs(1)).await;
        let mut batch = publisher.start_batch();
        val.update(&mut batch, &OPTIONS[i % OPTIONS.len()].to_string())?;
        batch.commit(None).await;
    }
    future::pending::<()>().await;
    Ok(())
}

async fn subscriber() -> Result<()> {
    println!("sub | connecting...");
    let mut con =
        time::timeout(Duration::from_secs(5), TcpStream::connect("127.0.0.1:7777"))
            .await??;
    con.set_nodelay(true)?;
    socket_channel::write_raw(&mut con, &MAGIC).await?;
    if socket_channel::read_raw::<u64, _>(&mut con).await? != MAGIC {
        bail!("incompatible protocol version")
    }
    socket_channel::write_raw(&mut con, &ClientHello { auth: AuthMethod::Anonymous })
        .await?;
    let chan = Channel::new(con);
    let (mut read_chan, mut write_chan) = chan.split();
    write_chan.send_one(&ToPublisher::Subscribe { path: "/hello".into() }).await?;
    loop {
        let msg: ToSubscriber = read_chan.receive().await?;
        println!("sub | received: {:?}", msg);
    }
    #[allow(unreachable_code)]
    Ok(())
}
