//! A batch stream read as an ordinary stream of `Vec`s: each fill gets an empty slot, and a filled
//! one is handed out whole.

use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use ruststream::BatchStream;
use ruststream_nats::NatsMessage;

/// The batches `stream` fills, each one yielded as the messages it held.
pub(crate) struct Lent<'s, Fill> {
    stream: Pin<&'s mut Fill>,
    slot: Vec<NatsMessage>,
}

impl<'s, Fill> Lent<'s, Fill> {
    pub(crate) const fn new(stream: Pin<&'s mut Fill>) -> Self {
        Self {
            stream,
            slot: Vec::new(),
        }
    }
}

// The slot is only moved, never pinned in place.
impl<Fill> Unpin for Lent<'_, Fill> {}

impl<Fill: BatchStream<Vec<NatsMessage>>> Stream for Lent<'_, Fill> {
    type Item = Result<Vec<NatsMessage>, Fill::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        this.stream
            .as_mut()
            .poll_fill(cx, &mut this.slot)
            .map(|filled| filled.map(|filled| filled.map(|()| mem::take(&mut this.slot))))
    }
}
