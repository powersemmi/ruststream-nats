//! The batch streams of [`NatsSubscriber`](super::NatsSubscriber): each fills the slot the
//! runtime lends, so a batch allocates nothing after the subscription starts.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use async_nats::jetstream::consumer::pull::{Batch, BatchError};
use futures::Stream;
use pin_project_lite::pin_project;
use ruststream::BatchStream;
use tracing::warn;

#[cfg(feature = "testing")]
use crate::in_process::Consumer;
#[cfg(feature = "testing")]
use crate::message::JetStreamMessage;
use crate::{error::NatsError, message::NatsMessage};

use super::jetstream_message;

pin_project! {
    /// One of two batch streams, chosen when the subscription opens its batches.
    #[project = SidesProjection]
    pub(super) enum Sides<Left, Right> {
        Left { #[pin] side: Left },
        Right { #[pin] side: Right },
    }
}

impl<Left, Right> Sides<Left, Right> {
    pub(super) const fn left(side: Left) -> Self {
        Self::Left { side }
    }

    pub(super) const fn right(side: Right) -> Self {
        Self::Right { side }
    }
}

impl<Slot, Left, Right> BatchStream<Slot> for Sides<Left, Right>
where
    Left: BatchStream<Slot>,
    Right: BatchStream<Slot, Error = Left::Error>,
{
    type Error = Left::Error;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Slot,
    ) -> Poll<Option<Result<(), Self::Error>>> {
        match self.project() {
            SidesProjection::Left { side } => side.poll_fill(cx, slot),
            SidesProjection::Right { side } => side.poll_fill(cx, slot),
        }
    }
}

pin_project! {
    /// `JetStream` batches: one batch is one pull request for at most `size` messages.
    ///
    /// The pull request in flight is held in place, and the messages it returns go straight into
    /// the lent slot, so a partial batch survives a dropped poll.
    pub(super) struct Pulls<Fetch, Fetching> {
        fetch: Fetch,
        #[pin]
        fetching: Option<Fetching>,
        batch: Option<Batch>,
        size: NonZeroUsize,
    }
}

impl<Fetch, Fetching> Pulls<Fetch, Fetching>
where
    Fetch: FnMut() -> Fetching,
    Fetching: Future<Output = Result<Batch, BatchError>>,
{
    /// Batches of at most `size`, each pulled by one call of `fetch`.
    pub(super) const fn new(fetch: Fetch, size: NonZeroUsize) -> Self {
        Self {
            fetch,
            fetching: None,
            batch: None,
            size,
        }
    }
}

impl<Fetch, Fetching> BatchStream<Vec<NatsMessage>> for Pulls<Fetch, Fetching>
where
    Fetch: FnMut() -> Fetching,
    Fetching: Future<Output = Result<Batch, BatchError>>,
{
    type Error = NatsError;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Vec<NatsMessage>,
    ) -> Poll<Option<Result<(), NatsError>>> {
        let mut this = self.project();
        loop {
            if let Some(batch) = this.batch.as_mut() {
                match ready!(Pin::new(batch).poll_next(cx)) {
                    Some(Ok(message)) => {
                        slot.push(jetstream_message(message));
                        if slot.len() < this.size.get() {
                            continue;
                        }
                    }
                    Some(Err(err)) if slot.is_empty() => {
                        *this.batch = None;
                        return Poll::Ready(Some(Err(NatsError::JetStream(err))));
                    }
                    Some(Err(err)) => warn!(
                        target: "ruststream::nats",
                        error = %err,
                        "jetstream fetch error mid-batch; delivering the partial batch",
                    ),
                    // An empty fetch only means `expires` elapsed with nothing pending.
                    None if slot.is_empty() => {
                        *this.batch = None;
                        continue;
                    }
                    None => {}
                }
                *this.batch = None;
                return Poll::Ready(Some(Ok(())));
            }
            let Some(fetching) = this.fetching.as_mut().as_pin_mut() else {
                this.fetching.set(Some((this.fetch)()));
                continue;
            };
            let fetched = ready!(fetching.poll(cx));
            this.fetching.set(None);
            match fetched {
                Ok(batch) => *this.batch = Some(batch),
                Err(err) => return Poll::Ready(Some(Err(NatsError::JetStream(Box::new(err))))),
            }
        }
    }
}

/// `JetStream` batches of the in-process transport: a fetch hands over what the consumer has, up
/// to `size`, and waits only for the first message, as a pull request does.
#[cfg(feature = "testing")]
pub(super) struct InProcessPulls<'a> {
    consumer: &'a mut Consumer,
    size: NonZeroUsize,
}

#[cfg(feature = "testing")]
impl<'a> InProcessPulls<'a> {
    pub(super) const fn new(consumer: &'a mut Consumer, size: NonZeroUsize) -> Self {
        Self { consumer, size }
    }
}

#[cfg(feature = "testing")]
impl BatchStream<Vec<NatsMessage>> for InProcessPulls<'_> {
    type Error = NatsError;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Vec<NatsMessage>,
    ) -> Poll<Option<Result<(), NatsError>>> {
        let this = self.get_mut();
        while slot.len() < this.size.get() {
            match this.consumer.poll_next(cx) {
                Poll::Ready(Some(delivery)) => slot.push(NatsMessage::JetStream(Box::new(
                    JetStreamMessage::in_process(delivery),
                ))),
                Poll::Ready(None) if slot.is_empty() => return Poll::Ready(None),
                Poll::Pending if slot.is_empty() => return Poll::Pending,
                Poll::Ready(None) | Poll::Pending => break,
            }
        }
        Poll::Ready(Some(Ok(())))
    }
}
