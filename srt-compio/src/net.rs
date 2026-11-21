use std::{
    convert::TryFrom,
    error,
    fmt::{Debug, Display, Formatter},
    io::{self, Cursor, ErrorKind},
    net::SocketAddr,
};

use compio::{
    net::{ToSocketAddrsAsync, UdpSocket},
    runtime::JoinHandle,
    BufResult,
};
use futures::{channel::mpsc, prelude::*};
use socket2::{Domain, Protocol, Socket, Type};
use srt_protocol::packet::{Packet, ReceivePacketResult};

use crate::options::*;

const MAX_DATAGRAM_SIZE: usize = 65536;
const RECEIVE_QUEUE_SIZE: usize = 1024;

pub async fn bind_socket(options: &SocketOptions) -> Result<UdpSocket, io::Error> {
    let socket = Socket::new(
        if options.connect.local.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        },
        Type::DGRAM,
        Some(Protocol::UDP),
    )?;

    let send_buffer_size = usize::try_from(options.connect.udp_send_buffer_size.0).unwrap();
    let recv_buffer_size = usize::try_from(options.connect.udp_recv_buffer_size.0).unwrap();

    socket.set_nonblocking(true)?;
    socket.set_recv_buffer_size(recv_buffer_size)?;
    socket.set_send_buffer_size(send_buffer_size)?;
    socket.bind(&options.connect.local.into())?;

    UdpSocket::from_std(socket.into())
}

pub async fn lookup_remote_host(remote: &SocketAddress) -> Result<SocketAddr, io::Error> {
    use SocketHost::*;
    let mut remote_address = match &remote.host {
        Domain(domain) => format!("{domain}:0")
            .to_socket_addrs_async()
            .await?
            .next()
            .ok_or_else(|| {
                io::Error::new(ErrorKind::NotFound, OptionsError::InvalidRemoteAddress)
            })?,
        Ipv4(ipv4) => SocketAddr::new((*ipv4).into(), 0),
        Ipv6(ipv6) => SocketAddr::new((*ipv6).into(), 0),
    };
    if remote_address.port() == 0 {
        remote_address.set_port(remote.port);
    }
    Ok(remote_address)
}

/// Packet oriented wrapper around a compio [`UdpSocket`].
///
/// Completion based I/O is not cancel safe in the way readiness based I/O is: dropping an
/// in-flight `recv_from` loses the buffer and any datagram it completed with. Since packets are
/// received inside `select!`s, the root `PacketSocket` runs a dedicated reader task that owns
/// the receive side and forwards parsed packets through a channel, which is cancel safe.
pub struct PacketSocket {
    socket: UdpSocket,
    stream: mpsc::Receiver<ReceivePacketResult>,
    buffer: Vec<u8>,
    reader: Option<JoinHandle<()>>,
}

impl PacketSocket {
    pub fn from_socket(socket: UdpSocket, buffer_capacity: usize) -> Self {
        let (sender, stream) = mpsc::channel(RECEIVE_QUEUE_SIZE);
        let reader = compio::runtime::spawn(Self::read_loop(socket.clone(), sender));
        Self {
            socket,
            stream,
            buffer: Vec::with_capacity(buffer_capacity),
            reader: Some(reader),
        }
    }

    pub fn clone_channel(
        &self,
        channel_buffer: usize,
    ) -> (mpsc::Sender<ReceivePacketResult>, Self) {
        let (packet_sender, packet_receiver) = mpsc::channel(channel_buffer);
        (
            packet_sender,
            Self {
                socket: self.socket.clone(),
                stream: packet_receiver,
                buffer: Vec::with_capacity(self.buffer.capacity()),
                reader: None,
            },
        )
    }

    pub async fn send(&mut self, packet: (Packet, SocketAddr)) -> Result<usize, io::Error> {
        let mut buffer = std::mem::take(&mut self.buffer);
        buffer.clear();
        packet.0.serialize(&mut buffer);
        let BufResult(result, buffer) = self.socket.send_to(buffer, packet.1).await;
        self.buffer = buffer;
        result
    }

    pub async fn receive(&mut self) -> ReceivePacketResult {
        self.stream.next().await.unwrap_or_else(|| {
            Err(io::Error::new(ErrorKind::NotConnected, PacketStreamClosedError).into())
        })
    }

    async fn read_loop(socket: UdpSocket, mut sender: mpsc::Sender<ReceivePacketResult>) {
        let is_ipv6 = match socket.local_addr() {
            Ok(addr) => addr.is_ipv6(),
            Err(e) => {
                let _ = sender.send(Err(e.into())).await;
                return;
            }
        };
        let mut buffer = Vec::with_capacity(MAX_DATAGRAM_SIZE);
        loop {
            buffer.clear();
            let BufResult(result, returned) = socket.recv_from(buffer).await;
            buffer = returned;
            let item = match result {
                Ok((size, from)) => Packet::parse(&mut Cursor::new(&buffer[0..size]), is_ipv6)
                    .map(|packet| (packet, from))
                    .map_err(Into::into),
                Err(e) => Err(e.into()),
            };
            if sender.send(item).await.is_err() {
                return;
            }
        }
    }
}

impl Drop for PacketSocket {
    fn drop(&mut self) {
        // dropping the handle cancels the reader task, releasing the socket
        let _ = self.reader.take();
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PacketStreamClosedError;

impl Display for PacketStreamClosedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "packet stream was closed")
    }
}

impl error::Error for PacketStreamClosedError {}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[compio::test]
    async fn resolve_dns() {
        let socket_address = SocketAddress {
            host: SocketHost::Domain("localhost".to_string()),
            port: 3000,
        };
        let remote_host = lookup_remote_host(&socket_address).await.unwrap();
        assert_matches!(
            remote_host.to_string().as_str(),
            "127.0.0.1:3000" | "[::1]:3000"
        );
    }
}
