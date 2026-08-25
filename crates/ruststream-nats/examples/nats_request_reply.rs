//! Request-reply over Core NATS: the Core publisher implements the `RequestReply` capability.
//!
//! `request` publishes with a reply inbox and resolves with the reply message, bounded by the
//! caller's timeout. The request runs from the scope's `after_startup` hook, where the `Publish`
//! policy is paired with the connected broker.
//!
//! Start a responder first (any NATS service; here the `nats` CLI), then run the example - after
//! startup it sends one request and prints the reply:
//!
//! ```text
//! nats reply questions 'pong'          # in another terminal
//! cargo run --example nats_request_reply -- run
//! ```

use std::io;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_nats::prelude::*;

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("requester", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.after_startup(Publish, async move |requester| -> io::Result<()> {
                // --8<-- [start:request]
                let reply = requester
                    .request(
                        OutgoingMessage::new("questions", b"what is the answer?"),
                        Duration::from_secs(2),
                    )
                    .await
                    .map_err(io::Error::other)?;
                println!("reply: {}", String::from_utf8_lossy(reply.payload()));
                // --8<-- [end:request]
                Ok(())
            });
        },
    )
}
