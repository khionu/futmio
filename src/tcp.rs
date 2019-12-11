use std::{
    io::{ErrorKind, Read, Result as IoResult, Write},
    net::{Shutdown, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{task::AtomicWaker, AsyncRead, AsyncWrite, Stream};
use log::{debug, error, trace};
use mio::{net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream}};

use crate::{SourceWaker, FutIoResult, PollDriver, Token};

/// This stream asynchronously yields incoming TCP connections.
pub struct TcpListenerStream {
    bundle: PollDriver,
    listener: MioTcpListener,
    _token: Token,
    waker_ptr: Arc<AtomicWaker>,
}

pub struct TcpConnection {
    bundle: PollDriver,
    stream: MioTcpStream,
    token: Token,
}

impl TcpListenerStream {
    pub fn bind(addr: &SocketAddr, poll_bundle: &PollDriver) -> IoResult<TcpListenerStream> {
        let listener = mio::net::TcpListener::bind(addr)?;
        let waker = SourceWaker::new(false);
        let waker_ptr = waker.get_read_waker();
        let token = poll_bundle.register(&listener, Ready::all(), PollOpt::edge(), waker)?;

        Ok(TcpListenerStream {
            bundle: poll_bundle.clone(),
            listener,
            _token: token,
            waker_ptr,
        })
    }
}

impl Stream for TcpListenerStream {
    type Item = IoResult<TcpConnection>;

    /// On Err, this returns the error from Mio.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.waker_ptr.register(cx.waker());

        match self.listener.accept() {
            // We have a connection!
            Ok(conn) => Some(TcpConnection::new(conn, &self.bundle)).into(),
            // We might be polled outside of the Bundle waking us.
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            // Oops
            Err(err) => Some(Err(err)).into(),
        }
    }
}

impl TcpConnection {
    fn new((stream, _): (MioTcpStream, SocketAddr), poll_bundle: &PollDriver) -> IoResult<Self> {
        let waker = SourceWaker::new(false);
        let token = poll_bundle.register(&stream, Ready::all(), PollOpt::edge(), waker)?;

        Ok(Self {
            bundle: poll_bundle.clone(),
            stream,
            token: token,
        })
    }

    pub fn connect(addr: &SocketAddr, poll_bundle: &PollDriver) -> IoResult<Self> {
        Self::new(
            (MioTcpStream::connect(addr)?, addr.clone()),
            poll_bundle,
        )
    }

    pub fn split(self) -> IoResult<(TcpSendStream, TcpRecvStream)> {
        let TcpConnection { bundle, stream, token } = self;
        let (tx_stream, rx_stream) = {
            let s = Arc::new(stream);
            let s2 = s.clone();
            (s, s2)
        };

        let waker = SourceWaker::new(true);
        let tx_waker = waker.get_write_waker();
        let rx_waker = waker.get_read_waker();

        let (tx_token, rx_token) = {
            let t = Arc::new(token);
            let t2 = t.clone();
            (t, t2)
        };

        let tx = TcpSendStream {
            stream: tx_stream,
            _token: tx_token,
            waker_ptr: tx_waker,
        };
        let rx = TcpRecvStream {
            stream: rx_stream,
            _token: rx_token,
            waker_ptr: rx_waker,
        };
        Ok((tx, rx))
    }

    /// See [`mio::net::TcpStream::peer_addr`] for documentation.
    pub fn peer_addr(&self) -> IoResult<SocketAddr> {
        self.stream.peer_addr()
    }

    /// See [`mio::net::TcpStream::local_addr`] for documentation.
    pub fn local_addr(&self) -> IoResult<SocketAddr> {
        self.stream.local_addr()
    }

    /// See [`mio::net::TcpStream::shutdown`] for documentation.
    pub fn shutdown(&self, how: Shutdown) -> IoResult<()> {
        self.stream.shutdown(how)
    }

    /// See [`mio::net::TcpStream::set_nodelay`] for documentation.
    pub fn set_nodelay(&self, nodelay: bool) -> IoResult<()> {
        self.stream.set_nodelay(nodelay)
    }

    /// See [`mio::net::TcpStream::nodelay`] for documentation.
    pub fn nodelay(&self) -> IoResult<bool> {
        self.stream.nodelay()
    }

    /// See [`mio::net::TcpStream::set_recv_buffer_size`] for documentation.
    pub fn set_recv_buffer_size(&self, size: usize) -> IoResult<()> {
        self.stream.set_recv_buffer_size(size)
    }

    /// See [`mio::net::TcpStream::recv_buffer_size`] for documentation.
    pub fn recv_buffer_size(&self) -> IoResult<usize> {
        self.stream.recv_buffer_size()
    }

    /// See [`mio::net::TcpStream::set_send_buffer_size`] for documentation.
    pub fn set_send_buffer_size(&self, size: usize) -> IoResult<()> {
        self.stream.set_send_buffer_size(size)
    }

    /// See [`mio::net::TcpStream::send_buffer_size`] for documentation.
    pub fn send_buffer_size(&self) -> IoResult<usize> {
        self.stream.send_buffer_size()
    }

    /// See [`mio::net::TcpStream::set_keepalive`] for documentation.
    pub fn set_keepalive(&self, keepalive: Option<Duration>) -> IoResult<()> {
        self.stream.set_keepalive(keepalive)
    }

    /// See [`mio::net::TcpStream::keepalive`] for documentation.
    pub fn keepalive(&self) -> IoResult<Option<Duration>> {
        self.stream.keepalive()
    }

    /// See [`mio::net::TcpStream::set_ttl`] for documentation.
    pub fn set_ttl(&self, ttl: u32) -> IoResult<()> {
        self.stream.set_ttl(ttl)
    }

    /// See [`mio::net::TcpStream::ttl`] for documentation.
    pub fn ttl(&self) -> IoResult<u32> {
        self.stream.ttl()
    }

    /// See [`mio::net::TcpStream::set_only_v6`] for documentation.
    pub fn set_only_v6(&self, only_v6: bool) -> IoResult<()> {
        self.stream.set_only_v6(only_v6)
    }

    /// See [`mio::net::TcpStream::only_v6`] for documentation.
    pub fn only_v6(&self) -> IoResult<bool> {
        self.stream.only_v6()
    }

    /// See [`mio::net::TcpStream::set_linger`] for documentation.
    pub fn set_linger(&self, dur: Option<Duration>) -> IoResult<()> {
        self.stream.set_linger(dur)
    }

    /// See [`mio::net::TcpStream::linger`] for documentation.
    pub fn linger(&self) -> IoResult<Option<Duration>> {
        self.stream.linger()
    }
}

pub struct TcpRecvStream {
    stream: Arc<MioTcpStream>,
    _token: Arc<Token>,
    waker_ptr: Arc<AtomicWaker>,
}

pub struct TcpSendStream {
    stream: Arc<MioTcpStream>,
    _token: Arc<Token>,
    waker_ptr: Arc<AtomicWaker>,
}

impl AsyncRead for TcpRecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<FutIoResult<usize>> {
        self.waker_ptr.register(cx.waker());

        match self.stream.read(buf) {
            Ok(len) => Ok(len).into(),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            Err(ref err) if err.kind() == ErrorKind::Interrupted => {
                { cx.waker().clone() }.wake();
                Poll::Pending
            }
            err => err.into(),
        }
    }
}

impl AsyncWrite for TcpSendStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<FutIoResult<usize>> {
        self.waker_ptr.register(cx.waker());

        match self.stream.write(buf) {
            Ok(len) => Ok(len).into(),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                trace!("Poll write would block, returning pending");
                Poll::Pending
            },
            Err(ref err) if err.kind() == ErrorKind::Interrupted => {
                debug!("Poll write was interrupted, returning pending with immediate wake up");
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            err => err.into(),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<FutIoResult<()>> {
        self.waker_ptr.register(cx.waker());

        match self.stream.flush() {
            Ok(_) => Ok(()).into(),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            Err(ref err) if err.kind() == ErrorKind::Interrupted => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            err => err.into(),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<FutIoResult<()>> {
        self.waker_ptr.register(cx.waker());

        match self.stream.shutdown(Shutdown::Write) {
            Ok(_) => Ok(()).into(),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            Err(ref err) if err.kind() == ErrorKind::Interrupted => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            err => err.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;
    use std::task::Poll;
    use std::thread;
    use std::time::Duration;

    use futures::executor::block_on;
    use futures::StreamExt;
    use futures::{pin_mut, AsyncReadExt, AsyncWriteExt};
    use log::*;
    use mio::net::TcpStream as MioTcpStream;

    use crate::tcp::*;
    use crate::tests::init_test_log;
    use crate::PollDriver;

    #[test]
    fn can_await_connections() {
        // Start prep work
        init_test_log();
        let bundle = PollDriver::new(None, 32).unwrap();

        let bind_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 44444);
        let mut listener = TcpListenerStream::bind(&bind_addr, &bundle).unwrap();

        let next_conn = listener.next();
        pin_mut!(next_conn);
        let mut ctx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        // End prep work

        // Ensure that the listener defaults to Pending
        if let Poll::Ready(_) = next_conn.as_mut().poll(&mut ctx) {
            panic!("Listener should not have a connection yet");
        }

        // Sleep, so the iteration, ergo waking, is done after we block.
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            bundle.iter()
        });
        MioTcpStream::connect(&bind_addr).unwrap();
        block_on(next_conn).unwrap().unwrap();
    }

    #[test]
    fn can_await_send_and_recv() {
        init_test_log();
        info!("Preparing test");
        let bundle = PollDriver::new(None, 32).unwrap();
        let bundle_copy = bundle.clone();
        let bind_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 44445);
        info!("Binding TcpListenerStream");
        let mut listener = TcpListenerStream::bind(&bind_addr, &bundle).unwrap();
        info!("Starting reactor");
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(20));
            bundle_copy.iter().unwrap();
        });
        info!("Connecting to server");
        let remote = TcpConnection::connect(&bind_addr, &bundle).unwrap();
        info!("Splitting remote");
        let (_, mut remote_rx) = remote.split().unwrap();
        info!("Blocking on recv connection");
        let local = block_on(listener.next()).unwrap().unwrap();
        info!("Splitting local");
        let (mut local_tx, _) = local.split().unwrap();

        let sample = String::from("This is a test").into_bytes();
        let mut recv_buffer = Vec::new();

        info!("Blocking on Send");
        let tx_size = block_on(local_tx.write(sample.as_slice())).unwrap();
        info!("Blocking on Recv");
        let rx_size = block_on(remote_rx.read(recv_buffer.as_mut_slice())).unwrap();
        assert_eq!(tx_size, rx_size, "Bytes sent don't match amount received");
        assert_eq!(
            sample, recv_buffer,
            "Bytes sent are not the same as the bytes received"
        );
    }
}
