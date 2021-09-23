use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};

use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::net::UdpSocket;

/// Multicast service discovery port
pub const MULTICAST_PORT: u16 = 5121;
lazy_static::lazy_static! {
    /// IPv4 address for sending/receiving discovery messages
    pub static ref MULTICAST_ADDR_V4: Ipv4Addr = "224.0.0.69".parse().unwrap();
    /// IPv6 address for sending/receiving discovery messages
    pub static ref MULTICAST_ADDR_V6: Ipv6Addr = "ff02::45".parse().unwrap();
}

/// Creates a regular socket bound to the given address
pub fn create_unicast_socket<S: Into<SocketAddr>>(addr: S, v6_only: bool) -> io::Result<UdpSocket> {
    let addr = addr.into();

    let socket = match addr {
        SocketAddr::V4(_) => Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?,

        SocketAddr::V6(_) => {
            let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
            socket.set_only_v6(v6_only)?;
            socket
        }
    };

    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;

    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(addr))?;

    let tokio_socket = UdpSocket::from_std(socket.into())?;

    Ok(tokio_socket)
}

/// Creates a multicast socket bound to the given address
pub fn create_multicast_socket<S: Into<SocketAddr>>(
    addr: S,
    v6_only: bool,
) -> io::Result<UdpSocket> {
    let addr: SocketAddr = addr.into();

    let sock = create_unicast_socket(addr, v6_only)?;

    if addr.is_ipv6() {
        let mcast_addr: Ipv6Addr = *MULTICAST_ADDR_V6;
        sock.join_multicast_v6(&mcast_addr, 0)?;
    } else {
        let mcast_addr: Ipv4Addr = *MULTICAST_ADDR_V4;
        sock.join_multicast_v4(mcast_addr, Ipv4Addr::UNSPECIFIED)?;
    }

    Ok(sock)
}
