//! Conversions between `RustStream` and `async-nats` types.

use bytes::Bytes;
use ruststream::HeaderMap;

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

pub(crate) fn headers_to_nats(headers: &HeaderMap) -> Option<async_nats::HeaderMap> {
    if headers.is_empty() {
        return None;
    }
    let mut map = async_nats::HeaderMap::new();
    for (name, value) in headers.iter() {
        if let Ok(text) = std::str::from_utf8(value) {
            map.insert(name, text);
        }
    }
    Some(map)
}
