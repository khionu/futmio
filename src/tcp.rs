use std::{
    io::{ErrorKind, Read, Result as IoResult, Write},
    net::{Shutdown, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{task::AtomicWaker, AsyncRead, AsyncWrite, Stream};
use log::error;
use mio::{
    net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream},
    PollOpt, Ready,
};

use crate::{EventedWaker, FutIoResult, PollBundle, Token};

/// This stream asynchronously yields incoming TCP connections.
pub struct TcpListenerStream {
    bundle: PollBundle,
    listener: MioTcpListener,
    _token: Token,
    waker_ptr: Arc<AtomicWaker>,
}

pub struct TcpConnection {
    bundle: PollBundle,
    stream: MioTcpStream,
}

impl TcpListenerStream {
    pub fn bind(addr: &SocketAddr, poll_bundle: &PollBundle) -> IoResult<TcpListenerStream> {
        let listener = mio::net::TcpListener::bind(addr)?;
        let waker = EventedWaker::new(false);
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
            Ok(conn) => Some(Ok(TcpConnection::new(conn, &self.bundle))).into(),
            // We might be polled outside of the Bundle waking us.
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            // Oops
            Err(err) => Some(Err(err)).into(),
        }
    }
}

impl TcpConnection {
    fn new((stream, _): (MioTcpStream, SocketAddr), poll_bundle: &PollBundle) -> Self {
        Self {
            bundle: poll_bundle.clone(),
            stream,
        }
    }

    pub fn connect(addr: &SocketAddr, poll_bundle: &PollBundle) -> IoResult<Self> {
        Ok(Self::new(
            (MioTcpStream::connect(addr)?, addr.clone()),
            poll_bundle,
        ))
    }

    pub fn split(self) -> IoResult<(TcpSendStream, TcpRecvStream)> {
        let TcpConnection { bundle, stream } = self;
        let tx_stream = stream.try_clone()?;
        let rx_stream = stream;
        let waker = EventedWaker::new(true);
        let tx_waker = waker.get_write_waker();
        let rx_waker = waker.get_read_waker();

        let (tx_token, rx_token) =
            match bundle.register(&tx_stream, Ready::all(), PollOpt::edge(), waker) {
                Ok(token) => {
                    let t = Arc::new(token);
                    let t2 = t.clone();
                    (t, t2)
                }
                Err(err) => {
                    error!("Error registering TxStream for split: {}", err);
                    return Err(err);
                }
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
    stream: MioTcpStream,
    _token: Arc<Token>,
    waker_ptr: Arc<AtomicWaker>,
}

pub struct TcpSendStream {
    stream: MioTcpStream,
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
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Poll::Pending,
            Err(ref err) if err.kind() == ErrorKind::Interrupted => {
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
    use crate::tcp::TcpListenerStream;
    use crate::PollBundle;
    use futures::executor::block_on;
    use futures::pin_mut;
    use futures::StreamExt;
    use std::future::Future;
    use std::net::{IpAddr, SocketAddr, TcpStream};
    use std::str::FromStr;
    use std::task::{Poll, Waker};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn can_await_connections() {
        let bundle = PollBundle::new(None, 32).unwrap();

        let bind_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 44444);
        let mut listener = TcpListenerStream::bind(&bind_addr, &bundle).unwrap();

        let next_conn = listener.next();
        pin_mut!(next_conn);
        let mut ctx = std::task::Context::from_waker(futures::task::noop_waker_ref());

        if let Poll::Ready(_) = next_conn.as_mut().poll(&mut ctx) {
            panic!("Listener should not have a connection yet");
        }

        TcpStream::connect(&bind_addr).unwrap();
        // Sleep, so the iteration, ergo waking, is done after we block.
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            bundle.iter()
        });
        block_on(next_conn).unwrap();
    }
}
