use std::io::{ErrorKind, Read, Result as IoResult, Write};
use std::net::{Shutdown, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::{AsyncRead, AsyncWrite, Stream};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use mio::{PollOpt, Ready};

use crate::{FutIoResult, PollBundle, Token};

/// This stream asynchronously yields incoming TCP connections.
pub struct TcpListenerStream {
    bundle: PollBundle,
    listener: MioTcpListener,
    _token: Token,
    waker_ptr: Arc<Mutex<Option<Waker>>>,
}

pub struct TcpConnection {
    bundle: PollBundle,
    stream: MioTcpStream,
}

impl TcpListenerStream {
    pub fn bind(addr: &SocketAddr, poll_bundle: &PollBundle) -> IoResult<TcpListenerStream> {
        let listener = mio::net::TcpListener::bind(addr)?;
        let waker_ptr = Arc::new(Mutex::new(None));
        let token =
            poll_bundle.register(&listener, Ready::all(), PollOpt::edge(), waker_ptr.clone())?;

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
        overwrite_waker_prt(cx, &self.waker_ptr);

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
        let tx_token = bundle.get_token();
        let rx_token = bundle.get_token();
        let tx_waker = Arc::new(Mutex::new(None));
        let rx_waker = Arc::new(Mutex::new(None));

        bundle.register(
            &tx_stream,
            Ready::writable(),
            PollOpt::edge(),
            tx_waker.clone(),
        )?;
        bundle.register(
            &rx_stream,
            Ready::readable(),
            PollOpt::edge(),
            rx_waker.clone(),
        )?;

        let tx = TcpSendStream {
            stream: tx_stream,
            _token: tx_token,
            waker_ptr: Arc::new(Mutex::new(None)),
        };
        let rx = TcpRecvStream {
            stream: rx_stream,
            _token: rx_token,
            waker_ptr: Arc::new(Mutex::new(None)),
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
    _token: Token,
    waker_ptr: Arc<Mutex<Option<Waker>>>,
}

pub struct TcpSendStream {
    stream: MioTcpStream,
    _token: Token,
    waker_ptr: Arc<Mutex<Option<Waker>>>,
}

impl AsyncRead for TcpRecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<FutIoResult<usize>> {
        overwrite_waker_prt(cx, &self.waker_ptr);

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
        overwrite_waker_prt(cx, &self.waker_ptr);

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
        overwrite_waker_prt(cx, &self.waker_ptr);

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
        overwrite_waker_prt(cx, &self.waker_ptr);

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

#[inline]
pub(crate) fn overwrite_waker_prt(cx: &mut Context, ptr: &Arc<Mutex<Option<Waker>>>) {
    let mut g = match ptr.lock() {
        Ok(g) => g,
        Err(psn) => psn.into_inner(),
    };
    *g = Some(cx.waker().clone());
}

#[cfg(test)]
mod tests {}
