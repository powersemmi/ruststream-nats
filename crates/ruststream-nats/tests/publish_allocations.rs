//! What one publish costs this crate over the `async-nats` loop it wraps, in allocated blocks.
//!
//! Both loops below publish the same body to the same subject through the same connection, so
//! everything the client allocates for itself appears in both counts and cancels. What is left is
//! this crate's own, and that is what the assertion is about. The count is taken over a run rather
//! than a single publish, because the client's command channel grows a block every so often.
//!
//! Skipped unless `NATS_TEST_URL` is set; see `tests/live/mod.rs`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use async_nats::Subject;
use bytes::Bytes;
use ruststream::{Broker, BytesMut, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_nats::{ConnectedNatsBroker, NatsBroker, NatsPublish};

mod live;

/// Publishes one measurement takes. Enough that the command channel's occasional block is a
/// fraction of a block per publish, and short enough to stay a test.
const RUNS: usize = 2_000;

const SUBJECT: &str = "publish.allocations";
const BODY: &[u8] = b"{\"id\":1}";

/// Blocks this crate may allocate per publish over what the raw client allocates.
///
/// Two, and both are the destination: the outgoing message only lends its name, so the name is
/// copied once here, and the client copies it again turning it into a subject of its own. Nothing
/// else costs a block - the payload travels as the buffer the framework wrote, an empty header
/// map has no wire form, and the connection is read rather than cloned.
const ALLOWED_PER_PUBLISH: usize = 2;

/// Counts this thread's allocations. A thread-local count rather than a global one: the client
/// drives its connection on tasks of its own, and their allocations are none of this
/// measurement's business.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

async fn connected_or_skip() -> Option<ConnectedNatsBroker> {
    let url = live::url("NATS_TEST_URL")?;
    match NatsBroker::new(url.as_str()).connect().await {
        Ok(connected) => Some(connected),
        Err(err) => {
            live::unreachable(&url, &err);
            None
        }
    }
}

/// A publish through this crate costs the client's own blocks plus what it takes to carry the
/// destination, and nothing else.
#[tokio::test]
async fn a_publish_costs_no_more_than_its_destination_over_the_client() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let publisher = connected.publisher(NatsPublish);
    let client = connected.client();

    // Warm both paths: the first publishes on a connection grow buffers later ones reuse.
    for _ in 0..16 {
        client
            .publish(Subject::from_static(SUBJECT), Bytes::from_static(BODY))
            .await
            .expect("publish");
        let msg = OutgoingMessage::produced(SUBJECT, BytesMut::from(BODY));
        publisher.publish(msg, None).await.expect("publish");
    }

    // Both runs are built before either is measured, so building them is outside both counts.
    let raw_bodies: Vec<Bytes> = (0..RUNS).map(|_| Bytes::from_static(BODY)).collect();
    let ours: Vec<OutgoingMessage<'_, BytesMut>> = (0..RUNS)
        .map(|_| OutgoingMessage::produced(SUBJECT, BytesMut::from(BODY)))
        .collect();

    let before = allocations();
    for body in raw_bodies {
        client
            .publish(Subject::from_static(SUBJECT), body)
            .await
            .expect("publish");
    }
    let raw = allocations() - before;

    let before = allocations();
    for msg in ours {
        publisher.publish(msg, None).await.expect("publish");
    }
    let wrapped = allocations() - before;

    let over_the_client = (wrapped.saturating_sub(raw)) / RUNS;
    assert!(
        over_the_client <= ALLOWED_PER_PUBLISH,
        "a publish may allocate {ALLOWED_PER_PUBLISH} blocks over the client and allocated \
         {over_the_client}, over the client's own {raw} for {RUNS} publishes",
    );
    connected.shutdown().await.expect("shutdown");
}
