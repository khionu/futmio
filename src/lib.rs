use std::{
    collections::HashMap,
    io::Result as IoResult,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    },
    time::Duration,
};

use futures::io::Error as FutIoError;
use futures::task::AtomicWaker;
use log::error;
use mio::{
    event::{Event, Events, Source},
    Interest, Poll, Registry, Token as MioToken,
};
use std::sync::mpsc::channel;

type FutIoResult<T> = Result<T, FutIoError>;

pub mod tcp;

pub struct PollDriver {
    events: Events,
    poll: mio::Poll,
    timeout: Option<Duration>,
    wakers: Arc<Mutex<HashMap<usize, SourceWaker>>>,
}

pub struct PollRegistry {
    mio_registry: Registry,
    // We use an atomic counter to ensure a unique backing value per Token. usize should be large
    // enough.
    token_counter: Arc<AtomicUsize>,
    token_freed: Arc<Mutex<Receiver<usize>>>,
    token_drop_box: Sender<usize>,
    wakers: Arc<Mutex<HashMap<usize, SourceWaker>>>,
}

impl PollRegistry {
    pub fn try_clone(&self) -> IoResult<Self> {
        Ok(Self {
            mio_registry: self.mio_registry.try_clone()?,
            token_counter: self.token_counter.clone(),
            token_freed: self.token_freed.clone(),
            token_drop_box: self.token_drop_box.clone(),
            wakers: self.wakers.clone(),
        })
    }
}

#[derive(Clone)]
pub struct SourceWaker {
    pub(crate) read: Arc<AtomicWaker>,
    pub(crate) write: Arc<AtomicWaker>,
}

impl SourceWaker {
    pub fn wake(&self, event: &Event) {
        if event.is_writable() {
            self.write.wake();
        }

        if event.is_readable() {
            self.read.wake();
        }
    }

    /// Creates a new [`SourceWaker`]. If passed [`true`], the wakers are separate. If [`false`],
    /// the wakers are the same.
    pub fn new() -> Self {
        let read = Arc::new(Default::default());
        let write = Arc::new(Default::default());

        SourceWaker { read, write }
    }

    pub fn get_read_waker(&self) -> Arc<AtomicWaker> {
        self.read.clone()
    }

    pub fn get_write_waker(&self) -> Arc<AtomicWaker> {
        self.write.clone()
    }
}

/// Token returned by the PollBundle on registration. Keep it with the registered handle, drop it
/// *after* the handle. This ensures that, internally, the corresponding [`mio::Token`] will be
/// freed when the handle is dropped.
///
/// # Contract
/// You must keep this Token alive just as long as the [`mio::event::Source`] handle.
pub struct Token {
    val: usize,
    drop_box: Sender<usize>,
    registry: PollRegistry,
}

impl Token {
    /// Generates a [`mio::Token`]
    pub(crate) fn get_mio(&self) -> MioToken {
        MioToken(self.val)
    }
}

impl PartialEq<MioToken> for Token {
    fn eq(&self, other: &MioToken) -> bool {
        self.val == other.0
    }
}

impl PartialEq<Token> for MioToken {
    fn eq(&self, other: &Token) -> bool {
        self.0 == other.val
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        // We don't care if it fails. We just need to try if it is possible.
        let _ = self.registry.wakers.lock().map(|mut g| g.remove(&self.val));
        let _ = self.drop_box.send(self.val);
    }
}

impl PollDriver {
    pub fn new(
        timeout: impl Into<Option<Duration>>,
        event_buf_size: usize,
    ) -> IoResult<(PollDriver, PollRegistry)> {
        let poll = Poll::new()?;
        let registry = poll.registry().try_clone()?;
        let wakers = Arc::new(Mutex::new(Default::default()));
        let (tx, rx) = channel();

        let driver = PollDriver {
            events: Events::with_capacity(event_buf_size),
            poll,
            timeout: timeout.into(),
            wakers: wakers.clone(),
        };
        let registry = PollRegistry {
            mio_registry: registry,
            token_counter: Arc::new(AtomicUsize::new(1)),
            token_freed: Arc::new(Mutex::new(rx)),
            token_drop_box: tx,
            wakers,
        };
        Ok((driver, registry))
    }

    /// Most likely, this crate will be used for creating a custom executor. Said executor should
    /// have a reactor that runs this in a loop. For error details, see [`mio::Poll::poll`].
    pub fn iter(&mut self) -> IoResult<()> {
        // Lock on events, for mutable, synchronous execution.
        // Do the poll
        self.poll.poll(&mut self.events, self.timeout)?;

        // We lock on this *now* because we want minimal contention with Futures updating their
        // wakers.
        let wakers = self.wakers.lock().expect("Poisoned waker store");
        for event in self.events.iter() {
            // We register the waker at the same time as the token, so this is basically guaranteed
            // to have a value.
            if let Some(waker) = wakers.get(&event.token().0) {
                waker.wake(event);
            } else {
                error!("Registered handler does not have a corresponding Waker. This is a bug.")
            }
        }
        Ok(())
    }
    /*
    /// Registers a [`mio::Evented`] handle in the wrapped [`mio::Poll`], along with a
    /// [`std::task::Waker`], allowing an async wrapper of the given handle to be woken. This
    /// abstracts the [`mio::Poll`] <-> [`mio::Token`] relationship to allow a reactor to wake
    /// wrapped handlers.
    ///
    /// # Panics
    /// This function may panic if there are more than [`usize`] concurrent handlers registered.
    /// This is highly unlikely in most practical cases, as process sharding is essentially
    /// guaranteed to be needed before that number of handlers is reached.
     */
}

impl PollRegistry {
    pub fn register<S: Source + ?Sized>(
        &self,
        handle: &mut S,
        interest: Interest,
        wakers: SourceWaker,
    ) -> IoResult<Token> {
        let token = self.get_token()?;
        self.mio_registry
            .register(handle, token.get_mio(), interest)?;
        self.wakers
            .lock()
            .expect("Poisoned PollRegistry")
            .insert(token.val, wakers);
        Ok(token)
    }

    fn get_token(&self) -> IoResult<Token> {
        // First we check the inbox for a freed token. If we have one, reuse it. However, most
        // likely we won't, so we catch the error and just get a fresh value.
        let val = match self
            .token_freed
            .lock()
            .expect("Poisoned token channel")
            .try_recv()
        {
            Err(_) => {
                let val = self.token_counter.fetch_add(1, Ordering::AcqRel);
                if val == 0 {
                    panic!("Token Counter overflow. Consider sharding your application");
                }
                val
            }
            Ok(val) => val,
        };

        Ok(Token {
            val,
            drop_box: self.token_drop_box.clone(),
            registry: self.try_clone()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use log::LevelFilter;

    pub fn init_test_log() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }
}
