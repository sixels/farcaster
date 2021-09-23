use std::{
    borrow::Cow,
    collections::HashSet,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    pin::Pin,
    time::Duration,
};

use tokio::time::{self, Instant};
use tokio_stream::{wrappers::IntervalStream, Stream, StreamExt};

use crate::{
    util::{create_unicast_socket, MULTICAST_ADDR_V4, MULTICAST_ADDR_V6, MULTICAST_PORT},
    DiscoveryEvent, DiscoveryStream, QueryMessage, ServiceMessage, StreamType,
};

enum QueryEvent {
    Service((ServiceMessage, SocketAddr)),
    Timeout(u64),
}

pub type QueryStream<'a> =
    Pin<Box<dyn Stream<Item = (ServiceMessage, SocketAddr)> + Send + Sync + 'a>>;

#[derive(Clone)]
pub struct DiscoveryClient {
    payload: QueryMessage,
}

impl DiscoveryClient {
    pub async fn query<'a, S: Into<Cow<'a, str>>>(
        service_name: S,
        wait_timeout: Duration,
    ) -> io::Result<QueryStream<'a>> {
        assert!(wait_timeout > Duration::from_secs(1));

        let service_name: Cow<'a, str> = service_name.into();

        let payload = QueryMessage {
            service: service_name.to_string(),
        };
        let raw_payload = payload.into_message().to_json()?;

        let (discovery_stream, stream_type) = DiscoveryStream::any_multicast()?;

        if stream_type == StreamType::Ipv6 {
            let sender = create_unicast_socket((Ipv6Addr::UNSPECIFIED, MULTICAST_PORT), true)?;
            sender
                .send_to(&raw_payload, (*MULTICAST_ADDR_V6, MULTICAST_PORT))
                .await?;
        } else {
            let sender = create_unicast_socket((Ipv4Addr::UNSPECIFIED, MULTICAST_PORT), false)?;
            sender
                .send_to(&raw_payload, (*MULTICAST_ADDR_V4, MULTICAST_PORT))
                .await?;
        };

        let start = Instant::now();
        let timeout_stream =
            IntervalStream::new(time::interval(Duration::from_secs(1))).map(move |now| {
                QueryEvent::Timeout(
                    wait_timeout
                        .checked_sub(now.duration_since(start))
                        .map_or(0, |s| s.as_secs()),
                )
            });
        let query_ev_stream = discovery_stream.filter_map(move |msg| {
            msg.ok()
                .map(|ev| match ev {
                    DiscoveryEvent::Service(ev) if ev.0.service == service_name => {
                        Some(QueryEvent::Service(ev))
                    }
                    _ => None,
                })
                .flatten()
        });

        let mut found_services = HashSet::new();
        let query_stream = query_ev_stream
            .merge(timeout_stream)
            .take_while(|ev| {
                if let QueryEvent::Timeout(timeout) = ev {
                    *timeout > 0
                } else {
                    true
                }
            })
            .filter_map(move |ev| {
                if let QueryEvent::Service(service) = ev {
                    found_services.insert(service.0.instance.clone()).then(|| service)
                } else {
                    None
                }
            });

        Ok(Box::pin(query_stream))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::JoinHandle;
    use tokio_stream::StreamExt;

    use crate::{server::DiscoveryServer, ServiceMessage};

    use super::DiscoveryClient;

    #[tokio::test]
    async fn query_a_single_service() {
        // start some servers
        let servers = start_background_servers(vec!["service#1", "service#2", "service#1"]);

        let mut services = DiscoveryClient::query("service#1", Duration::from_secs(3))
            .await
            .unwrap();

        let mut found_services = Vec::new();
        while let Some((service, _)) = services.next().await {
            found_services.push(service);
        }

        for server in servers {
            server.abort();
        }

        assert!(
            found_services.len() == 2,
            "Should've found {} services, got {}",
            2,
            found_services.len()
        );

        for service in found_services {
            assert!(&service.service == "service#1")
        }
    }

    #[tokio::test]
    async fn query_multiple_services() {
        let servers = start_background_servers(vec![
            "service#1",
            "service#2",
            "service#2",
            "service#4",
        ]);

        let mut all_services = {
            let query_service1 = DiscoveryClient::query("service#1", Duration::from_secs(2))
                .await
                .unwrap();
            let query_service2 = DiscoveryClient::query("service#2", Duration::from_secs(2))
                .await
                .unwrap();
            let query_service3 = DiscoveryClient::query("service#3", Duration::from_secs(2))
                .await
                .unwrap();

            query_service1.merge(query_service2).merge(query_service3)
        };

        let mut found_services: Vec<ServiceMessage> = Vec::new();
        while let Some((service, _)) = all_services.next().await {
            found_services.push(service)
        }

        for server in servers {
            server.abort();
        }

        assert!(
            found_services.len() == 3,
            "Sould've found {} services, got {}",
            3,
            found_services.len()
        );

        for service in found_services {
            assert!(&service.service == "service#1" || &service.service == "service#2");
        }
    }

    fn start_background_servers(service_names: Vec<&str>) -> Vec<JoinHandle<()>> {
        let mut server_handles = Vec::new();
        for (i, &name) in service_names.iter().enumerate() {
            let name = String::from(name);
            let mut server = DiscoveryServer::new(name, format!("instance#{}", i), 80).unwrap();

            server_handles.push(tokio::spawn(async move {
                server.start().await.unwrap();
            }));
        }

        server_handles
    }
}
