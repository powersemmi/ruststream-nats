//! NATS subject syntax: parsing, validation and pattern matching.
//!
//! A subject is a dot-separated sequence of tokens (`foo.bar.baz`). Tokens may contain any of
//! `A-Z a-z 0-9 - _`. Two wildcards are recognised in *patterns* only (never in concrete
//! publishable subjects):
//!
//! * `*` matches exactly one token at the position it appears in;
//! * `>` matches one or more remaining tokens and may appear only as the final token.
//!
//! See the upstream NATS docs at <https://docs.nats.io/nats-concepts/subjects> for the canonical
//! reference.

use std::fmt;

/// Errors raised while parsing a NATS subject or pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubjectError {
    /// The subject was empty or contained an empty token (e.g. `foo..bar` or a trailing dot).
    EmptyToken,
    /// A token contained a character outside the NATS alphabet (`A-Z a-z 0-9 - _`), or mixed a
    /// wildcard with literal characters within the same token.
    InvalidCharacter,
    /// `>` appeared in a position other than the final token.
    GtNotLast,
    /// A publishable subject (i.e. not a subscription pattern) contained `*` or `>`.
    WildcardInConcreteSubject,
}

impl fmt::Display for SubjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyToken => f.write_str("subject contains an empty token"),
            Self::InvalidCharacter => f.write_str("subject contains an invalid character"),
            Self::GtNotLast => f.write_str("`>` is only allowed as the final token"),
            Self::WildcardInConcreteSubject => {
                f.write_str("publishable subject must not contain wildcards")
            }
        }
    }
}

impl std::error::Error for SubjectError {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PatternToken {
    Literal(String),
    Star,
    Gt,
}

/// A compiled subscription pattern. Construct with [`SubjectPattern::parse`] and match concrete
/// subjects with [`SubjectPattern::matches`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubjectPattern {
    tokens: Vec<PatternToken>,
}

impl SubjectPattern {
    /// Parses a NATS subscription pattern. Accepts `*` and `>` wildcards under the rules above.
    pub(crate) fn parse(pattern: &str) -> Result<Self, SubjectError> {
        let raw = split_tokens(pattern)?;
        let last_idx = raw.len() - 1;
        let mut tokens = Vec::with_capacity(raw.len());
        for (idx, tok) in raw.into_iter().enumerate() {
            match tok {
                "*" => tokens.push(PatternToken::Star),
                ">" => {
                    if idx != last_idx {
                        return Err(SubjectError::GtNotLast);
                    }
                    tokens.push(PatternToken::Gt);
                }
                literal => {
                    validate_literal_token(literal)?;
                    tokens.push(PatternToken::Literal(literal.to_owned()));
                }
            }
        }
        Ok(Self { tokens })
    }

    /// Returns true when `subject` (a concrete, wildcard-free subject) matches this pattern.
    /// A malformed `subject` (empty, embedded wildcard, illegal character) yields `false` rather
    /// than panicking; callers that need explicit feedback should pre-validate via
    /// [`validate_concrete_subject`].
    pub(crate) fn matches(&self, subject: &str) -> bool {
        let Ok(parts) = split_tokens(subject) else {
            return false;
        };
        if parts.iter().any(|t| *t == "*" || *t == ">") {
            return false;
        }

        let mut p = 0;
        let mut s = 0;
        while p < self.tokens.len() && s < parts.len() {
            match &self.tokens[p] {
                PatternToken::Literal(lit) => {
                    if lit != parts[s] {
                        return false;
                    }
                    p += 1;
                    s += 1;
                }
                PatternToken::Star => {
                    p += 1;
                    s += 1;
                }
                PatternToken::Gt => {
                    return s < parts.len();
                }
            }
        }
        p == self.tokens.len() && s == parts.len()
    }
}

/// Validates that `subject` is publishable: non-empty tokens, no wildcards, NATS alphabet only.
pub(crate) fn validate_concrete_subject(subject: &str) -> Result<(), SubjectError> {
    let tokens = split_tokens(subject)?;
    for tok in tokens {
        if tok == "*" || tok == ">" {
            return Err(SubjectError::WildcardInConcreteSubject);
        }
        validate_literal_token(tok)?;
    }
    Ok(())
}

fn split_tokens(subject: &str) -> Result<Vec<&str>, SubjectError> {
    if subject.is_empty() {
        return Err(SubjectError::EmptyToken);
    }
    let tokens: Vec<&str> = subject.split('.').collect();
    if tokens.iter().any(|t| t.is_empty()) {
        return Err(SubjectError::EmptyToken);
    }
    Ok(tokens)
}

fn validate_literal_token(token: &str) -> Result<(), SubjectError> {
    if token.is_empty() {
        return Err(SubjectError::EmptyToken);
    }
    if !token
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(SubjectError::InvalidCharacter);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_table() {
        // (pattern, subject, expected)
        let cases: &[(&str, &str, bool)] = &[
            // exact matches
            ("foo", "foo", true),
            ("foo.bar", "foo.bar", true),
            ("foo.bar.baz", "foo.bar.baz", true),
            ("foo", "bar", false),
            ("foo.bar", "foo.baz", false),
            ("foo.bar", "foo", false),
            ("foo", "foo.bar", false),
            // single-token wildcard
            ("foo.*", "foo.bar", true),
            ("foo.*", "foo.baz", true),
            ("foo.*", "foo", false),
            ("foo.*", "foo.bar.baz", false),
            ("*.bar", "foo.bar", true),
            ("*.bar", "x.bar", true),
            ("*.bar", "foo.x", false),
            ("*", "foo", true),
            ("*", "foo.bar", false),
            ("*.*", "foo.bar", true),
            ("*.*", "foo", false),
            ("*.*", "foo.bar.baz", false),
            // tail wildcard `>`
            ("foo.>", "foo.bar", true),
            ("foo.>", "foo.bar.baz", true),
            ("foo.>", "foo.bar.baz.qux", true),
            ("foo.>", "foo", false),
            ("foo.>", "bar.baz", false),
            (">", "foo", true),
            (">", "foo.bar.baz", true),
            // mixed
            ("foo.*.baz", "foo.x.baz", true),
            ("foo.*.baz", "foo.x.y", false),
            ("foo.*.>", "foo.x.y", true),
            ("foo.*.>", "foo.x.y.z", true),
            ("foo.*.>", "foo.x", false),
            // case sensitivity
            ("Foo", "foo", false),
            ("Foo", "Foo", true),
        ];
        for (pattern, subject, expected) in cases {
            let pat = SubjectPattern::parse(pattern).expect("pattern parses");
            assert_eq!(
                pat.matches(subject),
                *expected,
                "pattern={pattern} subject={subject}"
            );
        }
    }

    #[test]
    fn parse_rejects_gt_not_last() {
        assert_eq!(
            SubjectPattern::parse("foo.>.bar"),
            Err(SubjectError::GtNotLast)
        );
        assert_eq!(SubjectPattern::parse(">.foo"), Err(SubjectError::GtNotLast));
    }

    #[test]
    fn parse_rejects_empty_tokens() {
        assert_eq!(SubjectPattern::parse(""), Err(SubjectError::EmptyToken));
        assert_eq!(
            SubjectPattern::parse("foo..bar"),
            Err(SubjectError::EmptyToken)
        );
        assert_eq!(SubjectPattern::parse("foo."), Err(SubjectError::EmptyToken));
        assert_eq!(SubjectPattern::parse(".foo"), Err(SubjectError::EmptyToken));
    }

    #[test]
    fn parse_rejects_invalid_chars() {
        assert_eq!(
            SubjectPattern::parse("foo bar"),
            Err(SubjectError::InvalidCharacter)
        );
        assert_eq!(
            SubjectPattern::parse("foo/bar"),
            Err(SubjectError::InvalidCharacter)
        );
        assert_eq!(
            SubjectPattern::parse("foo*"),
            Err(SubjectError::InvalidCharacter)
        );
        assert_eq!(
            SubjectPattern::parse("*foo"),
            Err(SubjectError::InvalidCharacter)
        );
    }

    #[test]
    fn matches_rejects_invalid_subjects() {
        let pat = SubjectPattern::parse(">").expect("pattern parses");
        assert!(!pat.matches(""));
        assert!(!pat.matches("foo."));
        assert!(!pat.matches("foo..bar"));
        assert!(!pat.matches("foo.*"));
        assert!(!pat.matches("foo.>"));
    }

    #[test]
    fn concrete_subject_validation() {
        assert!(validate_concrete_subject("foo.bar").is_ok());
        assert!(validate_concrete_subject("orders.created").is_ok());
        assert!(validate_concrete_subject("a_b-c.d_e-f").is_ok());

        assert_eq!(
            validate_concrete_subject("foo.*"),
            Err(SubjectError::WildcardInConcreteSubject)
        );
        assert_eq!(
            validate_concrete_subject("foo.>"),
            Err(SubjectError::WildcardInConcreteSubject)
        );
        assert_eq!(validate_concrete_subject(""), Err(SubjectError::EmptyToken));
        assert_eq!(
            validate_concrete_subject("foo bar"),
            Err(SubjectError::InvalidCharacter)
        );
    }
}
