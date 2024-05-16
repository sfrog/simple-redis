use crate::{
    cmd::{Command, CommandExecutor},
    Backend, RespDecode, RespEncode, RespError, RespFrame, SimpleError,
};
use anyhow::Result;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{info, warn};

#[derive(Debug)]
struct RespFrameCodec;

#[derive(Debug)]
struct RedisRequest {
    frame: RespFrame,
    backend: Backend,
}

#[derive(Debug)]
struct RedisResponse {
    frame: RespFrame,
}

pub async fn stream_handler(stream: TcpStream, backend: Backend) -> Result<()> {
    let mut framed = Framed::new(stream, RespFrameCodec);

    loop {
        let result: Result<Option<()>> = match framed.next().await {
            Some(Ok(frame)) => {
                let request = RedisRequest {
                    frame,
                    backend: backend.clone(),
                };
                let response = request_handler(request).await;
                // do not close the connection if there is an error in the request
                match response {
                    Ok(response) => {
                        framed.send(response.frame).await?;
                        Ok(Some(()))
                    }
                    Err(e) => Err(e),
                }
            }
            Some(Err(e)) => Err(e),
            None => Ok(None),
        };

        match result {
            Ok(Some(_)) => {
                info!("Request handled");
                continue;
            }
            Ok(None) => {
                return Ok(());
            }
            Err(e) => {
                // response with an error frame, otherwise the connection will be closed
                let response = RedisResponse {
                    frame: SimpleError::new(e.to_string()).into(),
                };
                warn!("Handle Exception: {:?}", e);
                framed.send(response.frame).await?;
            }
        }
    }
}

async fn request_handler(request: RedisRequest) -> Result<RedisResponse> {
    let (frame, backend) = (request.frame, request.backend);
    let cmd: Command = frame.try_into()?;
    info!("Executing command: {:?}", cmd);
    let ret = cmd.execute(&backend);
    info!("Command executed, response: {:?}", ret);
    Ok(RedisResponse { frame: ret })
}

impl Encoder<RespFrame> for RespFrameCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RespFrame, dst: &mut bytes::BytesMut) -> Result<()> {
        let encoded = item.encode();
        info!("Encoded Response: {:?}", String::from_utf8_lossy(&encoded));
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}

impl Decoder for RespFrameCodec {
    type Item = RespFrame;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>> {
        match RespFrame::decode(src) {
            Ok(frame) => Ok(Some(frame)),
            Err(RespError::NotComplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
