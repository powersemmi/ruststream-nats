//! NATS subject rules as the client and the server apply them: what a publish may name, what a
//! subscription may name, and which subjects a subscription matches.
//!
//! A subject is a dot-separated sequence of tokens. A token is any run of characters other than
//! the dot and whitespace, so `orders:v1` and `$SYS.x` are ordinary subjects. In a subscription
//! two tokens are wildcards, and only as whole tokens: `*` matches exactly one token, `>` matches
//! one or more trailing tokens and may only come last. `foo*` is a literal token.
//!
//! The rules follow <https://docs.nats.io/nats-concepts/subjects> and the checks `async-nats`
//! makes before a frame leaves the client.

use std::fmt;

/// A subject or a subscription the client or the server refuses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubjectError {
    /// Empty, or it contains a character that breaks the protocol frame (space, tab, CR, LF).
    Framing,
    /// A subscription with an empty token: a leading or trailing dot, or two dots in a row.
    EmptyToken,
    /// A subscription with `>` anywhere but the last token.
    TailNotLast,
}

impl fmt::Display for SubjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Framing => f.write_str("invalid subject: empty, or it contains whitespace"),
            Self::EmptyToken => f.write_str("invalid subject: it contains an empty token"),
            Self::TailNotLast => {
                f.write_str("invalid subject: `>` is only allowed as the last token")
            }
        }
    }
}

impl std::error::Error for SubjectError {}

/// Whether the frame can carry `value`: what `async-nats` checks on every publish, subscribe and
/// queue group before anything is written.
fn frames(value: &str) -> Result<(), SubjectError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
    {
        return Err(SubjectError::Framing);
    }
    Ok(())
}

/// Checks a publish subject the way the client does: anything the frame can carry. The server
/// takes an empty token or a wildcard token in a publish and matches it as a literal.
pub(crate) fn check_publish(subject: &str) -> Result<(), SubjectError> {
    frames(subject)
}

/// Checks a queue group name the way the client does.
pub(crate) fn check_queue_group(name: &str) -> Result<(), SubjectError> {
    frames(name)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Literal(String),
    One,
    Tail,
}

/// A compiled subscription subject: a literal subject, or a pattern with `*` and `>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubjectPattern {
    tokens: Vec<Token>,
}

impl SubjectPattern {
    /// Parses a subscription subject. It is refused where the client refuses it (framing, an
    /// empty token) and where the server does (`>` before the last token).
    pub(crate) fn parse(pattern: &str) -> Result<Self, SubjectError> {
        frames(pattern)?;
        let raw: Vec<&str> = pattern.split('.').collect();
        if raw.iter().any(|token| token.is_empty()) {
            return Err(SubjectError::EmptyToken);
        }
        let last = raw.len() - 1;
        let mut tokens = Vec::with_capacity(raw.len());
        for (index, token) in raw.into_iter().enumerate() {
            tokens.push(match token {
                "*" => Token::One,
                ">" if index == last => Token::Tail,
                ">" => return Err(SubjectError::TailNotLast),
                literal => Token::Literal(literal.to_owned()),
            });
        }
        Ok(Self { tokens })
    }

    /// Whether a message published to `subject` reaches this subscription. The published
    /// subject is matched token by token, its tokens taken literally.
    pub(crate) fn matches(&self, subject: &str) -> bool {
        let mut parts = subject.split('.');
        for token in &self.tokens {
            match token {
                Token::Tail => return parts.next().is_some(),
                Token::One => {
                    if parts.next().is_none() {
                        return false;
                    }
                }
                Token::Literal(literal) => {
                    if parts.next() != Some(literal.as_str()) {
                        return false;
                    }
                }
            }
        }
        parts.next().is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_table() {
        let cases: &[(&str, &str, bool)] = &[
            ("foo", "foo", true),
            ("foo.bar", "foo.bar", true),
            ("foo", "bar", false),
            ("foo.bar", "foo", false),
            ("foo", "foo.bar", false),
            ("foo.*", "foo.bar", true),
            ("foo.*", "foo", false),
            ("foo.*", "foo.bar.baz", false),
            ("*.bar", "foo.bar", true),
            ("*", "foo", true),
            ("*", "foo.bar", false),
            ("foo.>", "foo.bar", true),
            ("foo.>", "foo.bar.baz", true),
            ("foo.>", "foo", false),
            (">", "foo", true),
            (">", "foo.bar.baz", true),
            ("foo.*.>", "foo.x.y", true),
            ("foo.*.>", "foo.x", false),
            ("Foo", "foo", false),
            // Any character but the dot and whitespace belongs to a token.
            ("orders:v1.*", "orders:v1.created", true),
            ("$SYS.>", "$SYS.server", true),
            // A wildcard inside a token is a literal character.
            ("foo*", "foo*", true),
            ("foo*", "foox", false),
            // A published wildcard token is taken literally, and a subscription wildcard matches
            // it like any other token.
            ("foo.*", "foo.*", true),
            ("foo.bar", "foo.*", false),
        ];
        for (pattern, subject, expected) in cases {
            let parsed = SubjectPattern::parse(pattern).expect("pattern parses");
            assert_eq!(
                parsed.matches(subject),
                *expected,
                "pattern={pattern} subject={subject}"
            );
        }
    }

    #[test]
    fn a_subscription_refuses_what_the_client_or_the_server_refuses() {
        assert_eq!(SubjectPattern::parse(""), Err(SubjectError::Framing));
        assert_eq!(SubjectPattern::parse("foo bar"), Err(SubjectError::Framing));
        assert_eq!(
            SubjectPattern::parse("foo\tbar"),
            Err(SubjectError::Framing)
        );
        assert_eq!(
            SubjectPattern::parse("foo..bar"),
            Err(SubjectError::EmptyToken)
        );
        assert_eq!(SubjectPattern::parse(".foo"), Err(SubjectError::EmptyToken));
        assert_eq!(SubjectPattern::parse("foo."), Err(SubjectError::EmptyToken));
        assert_eq!(
            SubjectPattern::parse("foo.>.bar"),
            Err(SubjectError::TailNotLast)
        );
        assert!(SubjectPattern::parse("foo*").is_ok());
        assert!(SubjectPattern::parse("orders/eu").is_ok());
    }

    #[test]
    fn a_publish_takes_whatever_the_frame_carries() {
        for subject in [
            "orders.created",
            "orders:v1",
            "a..b",
            "foo.*",
            "unicode.\u{e9}",
        ] {
            assert!(check_publish(subject).is_ok(), "{subject}");
        }
        for subject in ["", "foo bar", "foo\r\n"] {
            assert_eq!(
                check_publish(subject),
                Err(SubjectError::Framing),
                "{subject:?}"
            );
        }
    }
}
