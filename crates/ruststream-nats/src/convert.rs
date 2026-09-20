//! Conversions between `RustStream` and `async-nats` types.

use std::str::{FromStr, from_utf8};

use async_nats::{HeaderName, HeaderValue};
use bytes::{Bytes, BytesMut};
use ruststream::{HeaderMap, OutgoingMessage};

use crate::error::NatsError;

pub(crate) fn headers_from_nats(map: Option<&async_nats::HeaderMap>) -> HeaderMap {
    let Some(map) = map else {
        return HeaderMap::new();
    };
    let mut headers = HeaderMap::new();
    for (name, values) in map.iter() {
        if let Some(first) = values.iter().next() {
            headers.insert(name.to_string(), Bytes::copy_from_slice(first.as_ref()));
        }
    }
    headers
}

/// An outgoing message as `async-nats` takes it: the subject, the payload the client keeps and
/// the headers it writes.
///
/// Every publish surface of this crate goes through here, so what the client is handed is
/// decided in one place.
///
/// # Errors
///
/// Returns [`NatsError::Publish`] when a header has no NATS form; see [`headers_to_nats`].
pub(crate) fn nats_parts(
    msg: OutgoingMessage<'_, BytesMut>,
) -> Result<(String, Bytes, Option<async_nats::HeaderMap>), NatsError> {
    let (subject, payload, headers) = msg.into_parts();
    let headers = headers_to_nats(&headers)?;
    Ok((subject.to_owned(), payload.freeze(), headers))
}

/// The framework's headers as NATS headers, or an error naming the one the protocol cannot carry.
///
/// A NATS header is text: the name is printable ASCII without a colon, and the value is a line,
/// so a byte string, a colon in a name and an embedded newline all have no wire form. The
/// framework's map holds arbitrary bytes under arbitrary names, so the two meet here, and what
/// does not fit fails the publish. It is checked rather than dropped because a message that
/// arrives without the header its sender set is a silent change of meaning - a routing key that
/// is gone decides a different dispatch lane, an idempotency key that is gone stops deduplicating.
///
/// # Errors
///
/// Returns [`NatsError::Publish`] naming the header and what is wrong with it.
pub(crate) fn headers_to_nats(
    headers: &HeaderMap,
) -> Result<Option<async_nats::HeaderMap>, NatsError> {
    if headers.is_empty() {
        return Ok(None);
    }
    let mut map = async_nats::HeaderMap::new();
    for (name, value) in headers.iter() {
        // The fallible conversions on purpose: the `From` impls behind `insert` assert instead,
        // so a header carrying a newline would panic inside a publish.
        let header = HeaderName::from_str(name).map_err(|_| {
            rejected(
                name,
                "a NATS header name is printable ASCII and carries no colon",
            )
        })?;
        let text = from_utf8(value).map_err(|_| {
            rejected(
                name,
                "a NATS header value is text, and this one is not UTF-8",
            )
        })?;
        let value = HeaderValue::from_str(text).map_err(|_| {
            rejected(
                name,
                "a NATS header value is one line, and this one contains a newline",
            )
        })?;
        map.insert(header, value);
    }
    Ok(Some(map))
}

fn rejected(name: &str, why: &str) -> NatsError {
    NatsError::Publish(format!("header `{name}` cannot be sent over NATS: {why}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(name: &str, value: impl Into<Bytes>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(name.to_owned(), value.into());
        headers
    }

    // A copy and a hand-over carry the same bytes, so only the address tells them apart.
    #[test]
    fn the_client_is_handed_the_buffer_the_framework_wrote() {
        let payload = BytesMut::from(&br#"{"id":1}"#[..]);
        let written_at = payload.as_ptr();

        let (subject, body, headers) =
            nats_parts(OutgoingMessage::produced("orders.created", payload))
                .expect("a message with no headers has a NATS form");

        assert_eq!(subject, "orders.created");
        assert!(headers.is_none());
        assert_eq!(
            body.as_ptr(),
            written_at,
            "async-nats keeps the payload, so the buffer travels into the client rather than \
             being copied into a second one",
        );
    }

    #[test]
    fn a_text_header_survives_the_conversion() {
        let map = headers_to_nats(&one("x-tenant", "acme"))
            .expect("plain text is what NATS headers carry")
            .expect("a non-empty map converts to a non-empty map");
        assert_eq!(map.get("x-tenant").map(HeaderValue::as_str), Some("acme"));
    }

    #[test]
    fn an_empty_map_has_no_nats_form() {
        assert!(
            headers_to_nats(&HeaderMap::new())
                .expect("an empty map is not an error")
                .is_none()
        );
    }

    // Each of these panicked inside the publish before, because the conversions behind
    // `HeaderMap::insert` assert. A header the protocol cannot carry now fails the publish and
    // names itself.
    #[test]
    fn a_header_the_protocol_cannot_carry_fails_the_publish() {
        for (name, value) in [
            ("x-note", b"line one\r\nline two".as_slice()),
            ("x:note", b"fine"),
            ("x-key", &[0xff, 0x01]),
        ] {
            let err = headers_to_nats(&one(name, Bytes::copy_from_slice(value)))
                .expect_err("this header has no NATS form");
            let message = err.to_string();
            assert!(
                message.contains(name),
                "the error must name the header it is about, got: {message}",
            );
        }
    }
}
