use std::{
    borrow::Cow,
    io,
    net::{Ipv4Addr, Ipv6Addr},
};

use tokio::net::UdpSocket;
use tokio_stream::StreamExt;

use crate::{
    util::{create_unicast_socket, MULTICAST_ADDR_V4, MULTICAST_ADDR_V6, MULTICAST_PORT},
    DiscoveryAnyStream, DiscoveryEvent, DiscoveryStream, ServiceMessage, StreamType,
};

pub struct DiscoveryServer {
    /// The stream for receiving messages
    stream: DiscoveryAnyStream,
    /// The sender socket
    sender: UdpSocket,
    /// The payload to send when a query message is received
    payload: ServiceMessage,
}

impl DiscoveryServer {
    /// Create a new Discovery Server for the given service
    pub fn new<'a, 'b, S, I>(service_name: S, instance: I, port: u16) -> io::Result<Self>
    where
        S: Into<Cow<'a, str>>,
        I: Into<Cow<'b, str>>,
    {
        let payload = ServiceMessage {
            service: service_name.into().into_owned(),
            instance: instance.into().into_owned(),
            port,
        };

        let (stream, stream_type) = DiscoveryStream::any_multicast()?;

        let sender = if stream_type == StreamType::Ipv6 {
            let addr = (Ipv6Addr::UNSPECIFIED, MULTICAST_PORT);
            create_unicast_socket(addr, true)?
        } else {
            let addr = (Ipv4Addr::UNSPECIFIED, MULTICAST_PORT);
            create_unicast_socket(addr, false)?
        };

        Ok(Self {
            stream,
            sender,
            payload,
        })
    }

    /// Start the Discovery Server
    pub async fn start<'a>(&'a mut self) -> io::Result<()> {
        let raw_payload = &self.payload.clone().into_message().to_json()?;

        self.announce(raw_payload.clone()).await?;

        while let Some(event) = self.stream.next().await {
            match event {
                Ok(DiscoveryEvent::Query((query, _))) => {
                    // send the payload when the query match the running service's name
                    if query.service == self.payload.service {
                        self.announce(raw_payload.clone()).await?;
                    }
                }
                Err(e) => {
                    println!("An error occured! Stopping the Discovery Server. {:?}", e);
                    break;
                }
                // ignore other service messages
                _ => (),
            }
        }

        Ok(())
    }

    async fn announce(&self, payload: Vec<u8>) -> io::Result<()> {
        if self.sender.local_addr()?.is_ipv6() {
            self.sender
                .send_to(&payload, (*MULTICAST_ADDR_V6, MULTICAST_PORT))
                .await?;
        } else {
            self.sender
                .send_to(&payload, (*MULTICAST_ADDR_V4, MULTICAST_PORT))
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::client::DiscoveryClient;

    use super::*;

    type Result<T> = ::std::result::Result<T, Box<dyn ::std::error::Error>>;

    #[tokio::test]
    async fn create_server() -> Result<()> {
        let mut server = DiscoveryServer::new("service_one", "random_name", 80)?;

        let handle = tokio::spawn(async move {
            server.start().await.unwrap();
        });

        assert_query("service_one", 1).await;

        handle.abort();

        Ok(())
    }

    #[tokio::test]
    async fn create_multiple_servers() -> Result<()> {
        let services = vec![
            DiscoveryServer::new("service#1", "instance#1", 80)?,
            DiscoveryServer::new("service#2", "instance#1", 8080)?,
            DiscoveryServer::new("service#3", "instance#2", 8081)?,
            DiscoveryServer::new("service#3", "instance#1", 8080)?,
        ];

        let mut handles = Vec::new();
        for mut service in services {
            handles.push(tokio::spawn(async move {
                service.start().await.unwrap();
            }));
        }

        let query_service1 = assert_query("service#1", 1);
        let query_service2 = assert_query("service#2", 1);
        let query_service3 = assert_query("service#3", 2);
        tokio::join!(query_service1, query_service2, query_service3);

        for handle in handles {
            handle.abort();
        }

        Ok(())
    }

    async fn assert_query(name: &str, qty: usize) {
        let mut services = DiscoveryClient::query(name, Duration::from_secs(2))
            .await
            .unwrap();

        let mut found_services = Vec::new();
        while let Some(service) = services.next().await {
            found_services.push(service.0);
        }

        assert!(
            found_services.len() == qty,
            "{}: should have {} running but found {}",
            name,
            qty,
            found_services.len()
        );
        for service in found_services {
            assert!(service.service == name)
        }
    }
}
