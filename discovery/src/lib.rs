use std::net::IpAddr;
use std::pin::Pin;
use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};

use serde::{Deserialize, Serialize};
use tokio::{io::ReadBuf, net::UdpSocket};
use tokio_stream::{Stream, StreamExt};
use util::create_unicast_socket;

use self::util::{create_multicast_socket, MULTICAST_PORT};

pub mod client;
pub mod server;

mod util;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ServiceMessage {
    /// The service name
    pub service: String,
    /// This instance name
    pub instance: String,
    /// The port the service is listening to
    pub port: u16,
}

impl ServiceMessage {
    /// Move the ServiceMessage into a DiscoveryMessage to be multicasted through the network
    pub fn into_message(self) -> DiscoveryMessage {
        DiscoveryMessage::Service(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct QueryMessage {
    /// The service name to query
    pub service: String,
}

impl QueryMessage {
    /// Move the QueryMessage into a DiscoveryMessage to be multicasted through the network.
    pub fn into_message(self) -> DiscoveryMessage {
        DiscoveryMessage::Query(self)
    }
}

#[derive(Serialize, Deserialize, Debug)]
/// A message recognized by the discovery system for querying or announcing services.
pub enum DiscoveryMessage {
    Query(QueryMessage),
    Service(ServiceMessage),
}

impl DiscoveryMessage {
    /// Serialize the message in JSON format.
    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }
}

#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// Sent when a service is created or queried
    Service((ServiceMessage, SocketAddr)),
    /// Sent to query for a service
    Query((QueryMessage, SocketAddr)),
}

/// A stream for receiving discovery messages
struct DiscoveryStream(UdpSocket);
/// A stream that might receive messages from both IPv4 and IPv6 sockets
type DiscoveryAnyStream = Pin<Box<dyn Stream<Item = io::Result<DiscoveryEvent>> + Send + Sync>>;

#[derive(Debug, Clone, Copy, PartialEq)]
enum StreamType {
    Ipv4,
    Ipv6,
    Both,
}

#[allow(dead_code)] // unicast should be helpful later
impl DiscoveryStream {
    /// Creates a DiscoveryStream listening on the given address.
    fn new_multicast<I: Into<IpAddr>>(addr: I, v6_only: bool) -> io::Result<Self> {
        Ok(Self(create_multicast_socket(
            (addr, MULTICAST_PORT),
            v6_only,
        )?))
    }

    /// Creates a DiscoveryStream for receiving multicast messages from IPv4 only.
    pub fn new_multicast_v4() -> io::Result<Self> {
        Self::new_multicast(Ipv4Addr::UNSPECIFIED, false)
    }

    /// Creates a DiscoveryStream for receiving multicast messages from IPv6.
    ///
    /// The socket is created with the ipv6_only attribute set according to the `v6_only` parameter; if true, it will only receive IPv6 messages;
    /// otherwise, it will receive both IPv4 and IPv6 messages.
    ///
    /// If IPv6 is not supported, it will return an error even if `v6_only` is false
    pub fn new_multicast_v6(v6_only: bool) -> io::Result<Self> {
        Self::new_multicast(Ipv6Addr::UNSPECIFIED, v6_only)
    }

    /// Creates a DiscoveryStream listening on the given address withouth joining the multicast group
    fn new_unicast<I: Into<IpAddr>>(addr: I, v6_only: bool) -> io::Result<Self> {
        Ok(Self(create_unicast_socket(
            (addr, MULTICAST_PORT),
            v6_only,
        )?))
    }

    /// Creates a DiscoveryStream for receiving unicast messages from IPv4 only.
    pub fn new_unicast_v4() -> io::Result<Self> {
        Self::new_unicast(Ipv4Addr::UNSPECIFIED, false)
    }

    /// Creates a DiscoveryStream with IPv6 support for receiving uniscast messages
    pub fn new_unicast_v6(v6_only: bool) -> io::Result<Self> {
        Self::new_unicast(Ipv6Addr::UNSPECIFIED, v6_only)
    }

    /// Creates a DiscoveryStream using both IPv4 and IPv6 whenever it's possible.
    pub fn any_multicast() -> io::Result<(DiscoveryAnyStream, StreamType)> {
        // create an ipv4 stream
        match Self::new_multicast_v4() {
            Ok(ipv4_stream) => {
                // try to receive ipv6 responses too
                if let Ok(ipv6_stream) = Self::new_multicast_v6(true) {
                    Ok((Box::pin(ipv4_stream.merge(ipv6_stream)), StreamType::Both))
                } else {
                    Ok((Box::pin(ipv4_stream), StreamType::Ipv4))
                }
            }
            Err(_) => Self::new_multicast_v6(false)
                .map(|s| (Box::pin(s) as DiscoveryAnyStream, StreamType::Ipv6)),
        }
    }

    /// Creates a DiscoveryStream using both IPv4 and IPv6 whenever it's possible.
    pub fn any_unicast() -> io::Result<(DiscoveryAnyStream, StreamType)> {
        // create an ipv4 stream
        match Self::new_unicast_v4() {
            Ok(ipv4_stream) => {
                // try to receive ipv6 responses too
                if let Ok(ipv6_stream) = Self::new_unicast_v6(true) {
                    Ok((Box::pin(ipv4_stream.merge(ipv6_stream)), StreamType::Both))
                } else {
                    Ok((Box::pin(ipv4_stream), StreamType::Ipv4))
                }
            }
            Err(_) => Self::new_unicast_v6(false)
                .map(|s| (Box::pin(s) as DiscoveryAnyStream, StreamType::Ipv6)),
        }
    }
}

impl Stream for DiscoveryStream {
    type Item = io::Result<DiscoveryEvent>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut buffer = [0; 512];
        let mut rbuffer = ReadBuf::new(&mut buffer);

        self.0.poll_recv_from(cx, &mut rbuffer).map(|result| {
            Some(result.and_then(|from| {
                serde_json::from_slice::<DiscoveryMessage>(rbuffer.filled())
                    .map(|msg| match msg {
                        DiscoveryMessage::Query(query_msg) => {
                            DiscoveryEvent::Query((query_msg, from))
                        }
                        DiscoveryMessage::Service(svc_msg) => {
                            DiscoveryEvent::Service((svc_msg, from))
                        }
                    })
                    .map_err(From::from)
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    //! These tests are mainly for testing device discovery in LAN

    use std::time::Duration;

    use tokio_stream::StreamExt;

    use crate::{client::DiscoveryClient, server::DiscoveryServer};

    #[ignore = "This test should never return"]
    #[tokio::test]
    async fn block_server() {
        let mut server = DiscoveryServer::new("test_service", "none", 1234).unwrap();
        server.start().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn query_block_server() {
        let mut query = DiscoveryClient::query("test_service", Duration::from_secs(30))
            .await
            .unwrap();

        println!("");
        while let Some(service) = query.next().await {
            println!("{:?}", service);
        }
    }
}
