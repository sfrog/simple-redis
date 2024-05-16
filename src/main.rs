use anyhow::Result;
use simple_redis::{network, Backend};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main()]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    info!("Listening on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    let backend = Backend::new();

    loop {
        let (socket, raddr) = listener.accept().await?;
        info!("Accepted connection from {}", raddr);

        let backend = backend.clone();

        tokio::spawn(async move {
            match network::stream_handler(socket, backend).await {
                Ok(_) => info!("Connection closed"),
                Err(e) => warn!("Stream handle error: {:?}", e),
            }
        });
    }
}
