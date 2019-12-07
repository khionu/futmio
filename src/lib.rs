use std::{
    collections::HashMap,
    io::Result as IoResult,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    },
    task::Waker,
    time::Duration,
};

use futures::io::Error as FutIoError;
use log::error;
use mio::{Evented, Events, PollOpt, Ready, Token as MioToken};

type FutIoResult<T> = Result<T, FutIoError>;

pub mod tcp;

#[derive(Clone)]
pub struct PollBundle {
    events: Arc<Mutex<Events>>,
    poll: Arc<mio::Poll>,
    timeout: Option<Duration>,
    // We use an atomic counter to ensure a unique backing value per Token. usize should be large
    // enough.
    token_counter: Arc<AtomicUsize>,
    token_freed: Arc<Mutex<Receiver<usize>>>,
    token_drop_box: Sender<usize>,
    wakers: Arc<Mutex<HashMap<usize, Arc<Mutex<Option<Waker>>>>>>,
}

/// Token returned by the PollBundle on registration. Keep it with the registered handle, drop it
/// *after* the handle. This ensures that, internally, the corresponding [`mio::Token`] will be
/// freed when the handle is dropped.
///
/// # Contract: You must keep this Token alive just as long as the [`mio::Evented`] handle.
pub struct Token {
    val: usize,
    drop_box: Sender<usize>,
    bundle: PollBundle,
}

impl Token {
    pub fn get_mio(&self) -> MioToken {
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
        let _ = self.bundle.wakers.lock().map(|mut g| g.remove(&self.val));
        let _ = self.drop_box.send(self.val);
    }
}

impl PollBundle {
    pub fn new(
        timeout: impl Into<Option<Duration>>,
        event_buf_size: usize,
    ) -> IoResult<PollBundle> {
        let (tx, rx) = std::sync::mpsc::channel();
        Ok(PollBundle {
            events: Arc::new(Mutex::new(Events::with_capacity(event_buf_size))),
            poll: Arc::new(mio::Poll::new()?),
            timeout: timeout.into(),
            token_counter: Arc::new(AtomicUsize::new(1)),
            token_freed: Arc::new(Mutex::new(rx)),
            token_drop_box: tx,
            wakers: Default::default(),
        })
    }

    fn get_token(&self) -> Token {
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

        Token {
            val,
            drop_box: self.token_drop_box.clone(),
            bundle: self.clone(),
        }
    }

    /// Most likely, this crate will be used for creating a custom executor. Said executor should
    /// have a reactor that runs this function in a loop. For error details, see
    /// [`mio::Poll::poll`].
    pub fn iter(&self) -> IoResult<()> {
        // Lock on events, for mutable, synchronous execution.
        let events = &mut *self.events.lock().expect("Poisoned PollBundle");
        // Do the poll
        self.poll.poll(events, self.timeout)?;

        // We lock on this *now* because we want minimal contention with Futures updating their
        // wakers.
        let wakers = self.wakers.lock().expect("Poisoned PollBundle");
        for event in events.iter() {
            // We register the waker at the same time as the token, so this is basically guaranteed
            // to have a value.
            if let Some(waker) = wakers.get(&event.token().0) {
                match waker.lock() {
                    // Wakey-wakey!!
                    Ok(w) => {
                        // If the Option<Waker> is None, they have yet to poll, and so will be
                        // polling of their own volition anyways. Ergo, having nothing to notify is
                        // harmless.
                        w.as_ref().map(Waker::wake_by_ref);
                    }
                    Err(_) => {
                        // If the Future is poisoned, we don't want to touch that.
                        error!("Ignoring panicked waker. This should be handled by the future.")
                    }
                }
            } else {
                error!("Registered handler does not have a corresponding Waker. This is a bug.")
            }
        }
        Ok(())
    }

    /// Registers a [`mio::Evented`] handle in the wrapped [`mio::Poll`], along with a
    /// [`std::task::Waker`], allowing an async wrapper of the given handle to be woken. This
    /// abstracts the [`mio::Poll`] <-> [`mio::Token`] relationship to allow a reactor to wake
    /// wrapped handlers.
    ///
    /// # Panics
    /// This function may panic if there are more than [`usize`] concurrent handlers registered.
    /// This is highly unlikely in most practical cases, as process sharding is essentially
    /// guaranteed to be needed before that number of handlers is reached.
    pub fn register<E: ?Sized>(
        &self,
        handle: &E,
        interest: Ready,
        opts: PollOpt,
        waker_ptr: Arc<Mutex<Option<Waker>>>,
    ) -> IoResult<Token>
    where
        E: Evented,
    {
        let token = self.get_token();
        self.poll
            .register(handle, token.get_mio(), interest, opts)?;
        self.wakers
            .lock()
            .expect("Poisoned PollBundle")
            .insert(token.val, waker_ptr);
        Ok(token)
    }
}

#[cfg(test)]
mod tests {}
