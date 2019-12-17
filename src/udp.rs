use std::{
    io::{self, ErrorKind, Result as IoResult},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    task::{Context, Poll},
};

use futures::future::poll_fn;
use mio::net::UdpSocket as MioUdpSocket;

use crate::{FutIoResult, PollRegistry, SourceWaker, Token, INTEREST_RW};

#[derive(Clone)]
pub struct UdpSocket {
    waker: SourceWaker,
    socket: Arc<MioUdpSocket>,
    token: Arc<Token>,
}

impl UdpSocket {
    pub fn bind(addr: SocketAddr, poll_registry: &PollRegistry) -> IoResult<Self> {
        let mut socket = mio::net::UdpSocket::bind(addr)?;
        let waker = SourceWaker::new();
        let token = poll_registry.register(&mut socket, INTEREST_RW, waker.clone())?;

        Ok(UdpSocket {
            waker,
            socket: Arc::new(socket),
            token: Arc::new(token),
        })
    }

    pub fn connect(&self, addr: SocketAddr) -> IoResult<()> {
        MioUdpSocket::connect(&self.socket, addr)
    }

    pub async fn recv(&self, buf: &mut [u8]) -> IoResult<usize> {
        poll_fn(|cx| self.poll_recv(cx, buf)).await
    }

    fn poll_recv(&self, cx: &mut Context, buf: &mut [u8]) -> Poll<FutIoResult<usize>> {
        self.waker.read.register(cx.waker());

        match self.socket.recv(buf) {
            Ok(len) => Ok(len).into(),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            Err(ref err) if err.kind() == ErrorKind::Interrupted => {
                cx.waker().clone().wake();
                Poll::Pending
            }
            err => err.into(),
        }
    }

    pub async fn send(&self, buf: &[u8]) -> IoResult<usize> {
        poll_fn(|cx| self.poll_send(cx, buf)).await
    }

    fn poll_send(&self, cx: &mut Context, buf: &[u8]) -> Poll<FutIoResult<usize>> {
        self.waker.write.register(cx.waker());

        match self.socket.send(buf) {
            Ok(len) => Ok(len).into(),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            Err(ref err) if err.kind() == ErrorKind::Interrupted => {
                cx.waker().clone().wake();
                Poll::Pending
            }
            err => err.into(),
        }
    }

    fn poll_recv_from(
        &self,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<FutIoResult<(usize, SocketAddr)>> {
        self.waker.read.register(cx.waker());

        match self.socket.recv_from(buf) {
            Ok(len) => Ok(len).into(),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            Err(ref err) if err.kind() == ErrorKind::Interrupted => {
                cx.waker().clone().wake();
                Poll::Pending
            }
            err => err.into(),
        }
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> IoResult<(usize, SocketAddr)> {
        poll_fn(|cx| self.poll_recv_from(cx, buf)).await
    }

    fn poll_send_to(
        &self,
        cx: &mut Context,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<FutIoResult<usize>> {
        self.waker.write.register(cx.waker());

        match self.socket.send_to(buf, target) {
            Ok(len) => Ok(len).into(),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            Err(ref err) if err.kind() == ErrorKind::Interrupted => {
                cx.waker().clone().wake();
                Poll::Pending
            }
            err => err.into(),
        }
    }

    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> IoResult<usize> {
        poll_fn(|cx| self.poll_send_to(cx, buf, target)).await
    }

    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        self.socket.set_broadcast(on)
    }

    pub fn broadcast(&self) -> io::Result<bool> {
        self.socket.broadcast()
    }

    pub fn set_multicast_loop_v4(&self, on: bool) -> io::Result<()> {
        self.socket.set_multicast_loop_v4(on)
    }

    pub fn multicast_loop_v4(&self) -> io::Result<bool> {
        self.socket.multicast_loop_v4()
    }

    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.socket.set_multicast_ttl_v4(ttl)
    }

    pub fn multicast_ttl_v4(&self) -> io::Result<u32> {
        self.socket.multicast_ttl_v4()
    }

    pub fn set_multicast_loop_v6(&self, on: bool) -> io::Result<()> {
        self.socket.set_multicast_loop_v6(on)
    }

    pub fn multicast_loop_v6(&self) -> io::Result<bool> {
        self.socket.multicast_loop_v6()
    }

    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.socket.set_ttl(ttl)
    }

    pub fn ttl(&self) -> io::Result<u32> {
        self.socket.ttl()
    }

    pub fn join_multicast_v4(&self, multiaddr: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
        self.socket.join_multicast_v4(multiaddr, interface)
    }

    pub fn join_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> io::Result<()> {
        self.socket.join_multicast_v6(multiaddr, interface)
    }

    pub fn leave_multicast_v4(&self, multiaddr: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
        self.socket.leave_multicast_v4(multiaddr, interface)
    }

    pub fn leave_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> io::Result<()> {
        self.socket.leave_multicast_v6(multiaddr, interface)
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.socket.take_error()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;
    use std::thread;
    use std::time::Duration;

    use futures::executor::block_on;
    use log::*;

    use crate::PollDriver;
    use crate::tests::init_test_log;
    use crate::udp::*;

    #[test]
    fn can_await_send_and_recv() {
        init_test_log();
        info!("Preparing test");
        let (mut driver, registry) = PollDriver::new(None, 32).unwrap();

        info!("Binding UdpSockets");
        let bind_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 44446);
        let local = UdpSocket::bind(bind_addr, &registry).unwrap();
        let bind_addr2 = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 44447);
        let remote = UdpSocket::bind(bind_addr2, &registry).unwrap();
        remote.connect(bind_addr).unwrap();

        info!("Starting reactor");
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(20));
            driver.iter().unwrap();
        });
        let sample = String::from("This is a test").into_bytes();
        let mut recv_buffer = [0; std::u16::MAX as usize];

        info!("Blocking on Send");
        let tx_size = block_on(remote.send(sample.as_slice())).unwrap();

        info!("Blocking on Recv");
        let rx_size = block_on(local.recv(&mut recv_buffer)).unwrap();
        eprintln!("recv_buffer.len(): {}", recv_buffer.len());
        assert_eq!(tx_size, rx_size, "Bytes sent don't match amount received");
        assert_eq!(
            sample.as_slice(),
            &recv_buffer[..14],
            "Bytes sent are not the same as the bytes received"
        );
    }

    #[test]
    fn can_await_send_to_and_recv_from() {
        init_test_log();
        info!("Preparing test");
        let (mut driver, registry) = PollDriver::new(None, 32).unwrap();

        info!("Binding UdpSockets");

        let bind_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 44448);
        let local = UdpSocket::bind(bind_addr, &registry).unwrap();
        let bind_addr2 = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 44449);
        let remote = UdpSocket::bind(bind_addr2, &registry).unwrap();

        info!("Starting reactor");
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(20));
            driver.iter().unwrap();
        });
        let sample = String::from("This is a test").into_bytes();
        let mut recv_buffer = [0; std::u16::MAX as usize];

        info!("Blocking on Send");
        let tx_size = block_on(remote.send_to(sample.as_slice(), bind_addr)).unwrap();

        info!("Blocking on Recv");
        let (rx_size, from_addr) = block_on(local.recv_from(&mut recv_buffer)).unwrap();
        eprintln!("recv_buffer.len(): {}", recv_buffer.len());
        assert_eq!(tx_size, rx_size, "Bytes sent don't match amount received");
        assert_eq!(
            sample.as_slice(),
            &recv_buffer[..14],
            "Bytes sent are not the same as the bytes received"
        );
        assert_eq!(
            bind_addr2, from_addr,
            "Sent from address is not the same as received from address"
        );
    }
}
