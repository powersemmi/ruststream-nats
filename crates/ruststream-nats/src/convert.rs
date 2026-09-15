//! Conversions between `RustStream` and `async-nats` types.

use std::str::{FromStr, from_utf8};

use async_nats::{HeaderName, HeaderValue};
use bytes::Bytes;
use ruststream::HeaderMap;

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
