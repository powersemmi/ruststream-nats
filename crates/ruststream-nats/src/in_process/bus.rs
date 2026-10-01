//! The in-process server: the subscriptions, the streams and their consumers, and the log of
//! what was published.

use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug, Formatter};
use std::str::from_utf8;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::jetstream::consumer::DeliverPolicy;
use bytes::Bytes;
use ruststream::testing::Coordinator;
use ruststream::{HeaderMap, RawMessage};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::delivery::{BusDelivery, ConsumerState, DeliverySender, Feed, Release};
use super::subject::{SubjectPattern, check_publish};
use crate::convert::headers_to_nats;
use crate::error::NatsError;
use crate::jetstream::{
    EXPECTED_LAST_MESSAGE_ID, EXPECTED_LAST_SEQUENCE, EXPECTED_LAST_SUBJECT_SEQUENCE,
    EXPECTED_STREAM, MESSAGE_ID,
};

/// The largest message a server takes unless it is configured otherwise, headers included.
const MAX_PAYLOAD: usize = 1024 * 1024;

/// How long a stream remembers a `Nats-Msg-Id` unless it is configured otherwise.
const DUPLICATE_WINDOW: Duration = Duration::from_secs(120);

/// The part of the connection options that decides what the server delivers.
#[derive(Debug, Clone)]
pub(crate) struct Settings {
    /// The server does not deliver a message back to the connection that published it.
    pub(crate) no_echo: bool,
    /// The prefix of the inbox a request's reply comes back on.
    pub(crate) inbox_prefix: String,
}

/// Who put a message on the bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    /// This connection: a publisher of the connected broker.
    Connection,
    /// Anyone else: the test's injected input, a stream answering a request.
    External,
}

/// How a publish is made: plainly, or through the `JetStream` API, which requires a stream and
/// reports a refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishMode {
    Core,
    JetStream,
}

/// Where a stream stored a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stored {
    pub(crate) stream: String,
    pub(crate) sequence: u64,
    pub(crate) duplicate: bool,
}

/// A set of subscriptions the server hands each message to one of.
///
/// A queue group is keyed by its name alone: the server groups queue subscriptions by name over
/// every subject that matches, so one name on `orders.*` and on `orders.>` is one group. A
/// durable consumer is keyed by its stream and name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Group {
    Queue(String),
    Durable { stream: String, name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    /// A Core subscription a dispatch loop reads.
    Core,
    /// A request's reply inbox, which the requester reads.
    Inbox,
    /// A subscription reading a consumer of `stream`.
    JetStream { stream: String },
}

struct Subscription {
    pattern: SubjectPattern,
    group: Option<Group>,
    sender: DeliverySender,
    kind: Kind,
}

struct StoredMessage {
    sequence: u64,
    message: async_nats::Message,
    at: SystemTime,
}

/// A stream as the bus knows it: the filters its consumers read, and what it stored.
struct Stream {
    name: String,
    filters: Vec<SubjectPattern>,
    messages: Vec<StoredMessage>,
    last_sequence: u64,
    last_message_id: Option<String>,
    subject_sequences: HashMap<String, u64>,
    message_ids: HashMap<String, (u64, Instant)>,
}

impl Stream {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            filters: Vec::new(),
            messages: Vec::new(),
            last_sequence: 0,
            last_message_id: None,
            subject_sequences: HashMap::new(),
            message_ids: HashMap::new(),
        }
    }

    fn captures(&self, subject: &str) -> bool {
        self.filters.iter().any(|filter| filter.matches(subject))
    }

    /// Stores `message` the way a stream does: a repeated `Nats-Msg-Id` inside the duplicate
    /// window is acknowledged and not stored, and a stated expectation that does not hold
    /// refuses the message.
    fn store(
        &mut self,
        message: &async_nats::Message,
        headers: &HeaderMap,
    ) -> Result<Stored, String> {
        let now = Instant::now();
        self.message_ids
            .retain(|_, (_, at)| now.duration_since(*at) < DUPLICATE_WINDOW);
        let message_id = text(headers, MESSAGE_ID);
        if let Some(id) = &message_id
            && let Some((sequence, _)) = self.message_ids.get(id)
        {
            return Ok(Stored {
                stream: self.name.clone(),
                sequence: *sequence,
                duplicate: true,
            });
        }
        if let Some(expected) = number(headers, EXPECTED_LAST_SEQUENCE)?
            && expected != self.last_sequence
        {
            return Err(format!("wrong last sequence: {}", self.last_sequence));
        }
        let subject = message.subject.as_str();
        if let Some(expected) = number(headers, EXPECTED_LAST_SUBJECT_SEQUENCE)? {
            let last = self.subject_sequences.get(subject).copied().unwrap_or(0);
            if expected != last {
                return Err(format!("wrong last sequence: {last}"));
            }
        }
        if let Some(expected) = text(headers, EXPECTED_LAST_MESSAGE_ID)
            && self.last_message_id.as_deref() != Some(expected.as_str())
        {
            return Err(format!(
                "wrong last msg ID: {}",
                self.last_message_id.as_deref().unwrap_or_default()
            ));
        }
        self.last_sequence += 1;
        let sequence = self.last_sequence;
        self.subject_sequences.insert(subject.to_owned(), sequence);
        if let Some(id) = message_id {
            self.message_ids.insert(id.clone(), (sequence, now));
            self.last_message_id = Some(id);
        }
        self.messages.push(StoredMessage {
            sequence,
            message: message.clone(),
            at: SystemTime::now(),
        });
        Ok(Stored {
            stream: self.name.clone(),
            sequence,
            duplicate: false,
        })
    }

    /// What a new consumer reading `filter` from `policy` on is handed first.
    fn backlog(&self, filter: &SubjectPattern, policy: DeliverPolicy) -> Vec<&StoredMessage> {
        let matching = self
            .messages
            .iter()
            .filter(|stored| filter.matches(stored.message.subject.as_str()));
        match policy {
            DeliverPolicy::All => matching.collect(),
            DeliverPolicy::New => Vec::new(),
            DeliverPolicy::Last => matching.last().into_iter().collect(),
            DeliverPolicy::LastPerSubject => {
                let mut last: BTreeMap<u64, &StoredMessage> = BTreeMap::new();
                let mut by_subject: HashMap<&str, u64> = HashMap::new();
                for stored in matching {
                    if let Some(previous) =
                        by_subject.insert(stored.message.subject.as_str(), stored.sequence)
                    {
                        last.remove(&previous);
                    }
                    last.insert(stored.sequence, stored);
                }
                last.into_values().collect()
            }
            DeliverPolicy::ByStartSequence { start_sequence } => matching
                .filter(|stored| stored.sequence >= start_sequence)
                .collect(),
            DeliverPolicy::ByStartTime { start_time } => {
                let start = start_time.unix_timestamp_nanos();
                matching
                    .filter(|stored| {
                        stored.at.duration_since(UNIX_EPOCH).map_or(0, |since| {
                            i128::try_from(since.as_nanos()).unwrap_or(i128::MAX)
                        }) >= start
                    })
                    .collect()
            }
        }
    }
}

fn text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| from_utf8(value).ok())
        .map(str::to_owned)
}

fn number(headers: &HeaderMap, name: &str) -> Result<Option<u64>, String> {
    text(headers, name)
        .map(|value| {
            value
                .trim()
                .parse()
                .map_err(|_| format!("invalid {name}: {value}"))
        })
        .transpose()
}

#[derive(Default)]
struct State {
    /// By id, which is the order the subscriptions were opened in.
    subscriptions: BTreeMap<u64, Subscription>,
    log: HashMap<String, Vec<RawMessage>>,
    /// How many messages each group has taken, so the next goes to the next member. Rotation
    /// rather than a random pick: a test that asserts the split sees the same split every run.
    turns: HashMap<Group, u64>,
    /// In the order the bus learned of them.
    streams: Vec<Stream>,
    durables: HashMap<(String, String), Arc<ConsumerState>>,
    /// What a durable consumer holds while no subscription reads it: the deliveries its last
    /// subscription left unread or unsettled. The next subscription on the durable receives them.
    parked: HashMap<Group, Vec<Parked>>,
}

/// A delivery a durable consumer holds with no subscription to hand it to. It carries no count:
/// the harness does not wait for a message nothing can read.
struct Parked {
    message: async_nats::Message,
    sequence: u64,
    delivered: u64,
}

impl State {
    fn stream_mut(&mut self, name: &str) -> &mut Stream {
        let index = if let Some(index) = self.streams.iter().position(|s| s.name == name) {
            index
        } else {
            self.streams.push(Stream::new(name));
            self.streams.len() - 1
        };
        &mut self.streams[index]
    }

    /// Whether a consumer of the app has named the stream, so the bus knows what it serves.
    fn knows(&self, stream: &str) -> bool {
        self.streams
            .iter()
            .any(|known| known.name == stream && !known.filters.is_empty())
    }

    /// The stream that stores a message published to `subject`: the first one a consumer's
    /// filter places the subject in. Streams do not overlap on a server, so one is all there is.
    fn capturing(&self, subject: &str) -> Option<usize> {
        self.streams
            .iter()
            .position(|stream| stream.captures(subject))
    }

    /// Who a message on `subject` reaches: every matching subscription outside a group, one
    /// member of each matching group in turn, and the consumers of the stream that stored it.
    fn recipients(
        &mut self,
        subject: &str,
        stored: Option<&str>,
        echo: bool,
    ) -> Vec<(DeliverySender, Kind)> {
        let mut recipients = Vec::new();
        let mut competing: BTreeMap<Group, Vec<(DeliverySender, Kind)>> = BTreeMap::new();
        for subscription in self.subscriptions.values() {
            let reaches = match &subscription.kind {
                Kind::Core | Kind::Inbox => echo,
                Kind::JetStream { stream } => stored == Some(stream.as_str()),
            };
            if !reaches || !subscription.pattern.matches(subject) {
                continue;
            }
            let entry = (subscription.sender.clone(), subscription.kind.clone());
            match &subscription.group {
                None => recipients.push(entry),
                Some(group) => competing.entry(group.clone()).or_default().push(entry),
            }
        }
        for (group, mut members) in competing {
            let turn = self.turns.entry(group).or_insert(0);
            let picked = usize::try_from(*turn % members.len() as u64).unwrap_or(0);
            *turn = turn.wrapping_add(1);
            recipients.push(members.swap_remove(picked));
        }
        recipients
    }
}

/// What a `JetStream` subscription asks the bus for.
pub(crate) struct ConsumerSpec<'a> {
    pub(crate) stream: &'a str,
    pub(crate) durable: Option<&'a str>,
    pub(crate) filter: SubjectPattern,
    pub(crate) deliver_policy: DeliverPolicy,
}

/// The in-process server one connected broker speaks to. Every publisher paired off that broker
/// shares it; distinct brokers share nothing.
pub(crate) struct Bus {
    closed: AtomicBool,
    /// What the connection published and what it was handed, for the closed broker's counters.
    sent: AtomicU64,
    received: AtomicU64,
    coordinator: OnceLock<Coordinator>,
    settings: Settings,
    /// The runtime the broker connected on, where a delayed redelivery's timer runs whichever
    /// thread settles.
    runtime: Handle,
    next_id: AtomicU64,
    state: Mutex<State>,
}

impl Debug for Bus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bus")
            .field("closed", &self.is_closed())
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl Bus {
    pub(crate) fn new(settings: Settings, runtime: Handle) -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            coordinator: OnceLock::new(),
            settings,
            runtime,
            next_id: AtomicU64::new(1),
            state: Mutex::new(State::default()),
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("in-process NATS state mutex poisoned")
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    pub(crate) fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.get().cloned()
    }

    /// The runtime the broker connected on.
    pub(crate) const fn runtime(&self) -> &Handle {
        &self.runtime
    }

    /// Whether the connection was opened with `no_echo`.
    pub(crate) const fn no_echo(&self) -> bool {
        self.settings.no_echo
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// `Ok` while the connection is open, [`NatsError::Closed`] once it has shut down.
    pub(crate) fn ensure_live(&self, subject: &str) -> Result<(), NatsError> {
        if self.is_closed() {
            return Err(NatsError::Closed {
                subject: subject.to_owned(),
            });
        }
        Ok(())
    }

    /// Shuts the connection: every subscription ends, and every handle aliasing it errors. Returns
    /// how many messages the connection published and how many it was handed.
    pub(crate) fn close(&self) -> (u64, u64) {
        let mut state = self.state();
        self.closed.store(true, Ordering::Release);
        state.subscriptions.clear();
        state.parked.clear();
        drop(state);
        (
            self.sent.load(Ordering::Relaxed),
            self.received.load(Ordering::Relaxed),
        )
    }

    fn register(
        self: &Arc<Self>,
        state: &mut State,
        pattern: SubjectPattern,
        group: Option<Group>,
        kind: Kind,
    ) -> (Feed, DeliverySender) {
        let (sender, rx) = mpsc::unbounded_channel();
        let id = self.next_id();
        state.subscriptions.insert(
            id,
            Subscription {
                pattern,
                group,
                sender: sender.clone(),
                kind,
            },
        );
        (Feed::new(rx, id, Arc::clone(self)), sender)
    }

    /// Opens a Core subscription, in the queue group `queue` when one is named.
    pub(crate) fn subscribe_core(
        self: &Arc<Self>,
        pattern: SubjectPattern,
        queue: Option<&str>,
    ) -> Feed {
        let group = queue.map(|name| Group::Queue(name.to_owned()));
        let mut state = self.state();
        self.register(&mut state, pattern, group, Kind::Core).0
    }

    /// Opens a subscription on a consumer: the durable one of that name when it exists already,
    /// a new one otherwise. A new consumer is first handed what its stream stored, from where its
    /// deliver policy says to start.
    pub(crate) fn subscribe_jetstream(
        self: &Arc<Self>,
        spec: ConsumerSpec<'_>,
    ) -> (Feed, DeliverySender, Arc<ConsumerState>) {
        let mut state = self.state();
        let stream = state.stream_mut(spec.stream);
        if !stream.filters.contains(&spec.filter) {
            stream.filters.push(spec.filter.clone());
        }
        let (consumer, existed) = if let Some(name) = spec.durable {
            let key = (spec.stream.to_owned(), name.to_owned());
            if let Some(consumer) = state.durables.get(&key) {
                (Arc::clone(consumer), true)
            } else {
                let consumer = ConsumerState::new(spec.stream, name, true);
                state.durables.insert(key, Arc::clone(&consumer));
                (consumer, false)
            }
        } else {
            let name = format!("{}_ephemeral_{}", spec.stream, self.next_id());
            (ConsumerState::new(spec.stream, &name, false), false)
        };
        let backlog: Vec<(async_nats::Message, u64)> = if existed {
            Vec::new()
        } else {
            state
                .stream_mut(spec.stream)
                .backlog(&spec.filter, spec.deliver_policy)
                .into_iter()
                .map(|stored| (stored.message.clone(), stored.sequence))
                .collect()
        };
        let group = spec.durable.map(|name| Group::Durable {
            stream: spec.stream.to_owned(),
            name: name.to_owned(),
        });
        // The client creates or updates the consumer: a durable has one filter, the one it was
        // last opened with, and every subscription on it reads through that filter, what the
        // durable already holds included.
        if let Some(group) = group.as_ref().filter(|_| existed) {
            for member in state.subscriptions.values_mut() {
                if member.group.as_ref() == Some(group) {
                    member.pattern = spec.filter.clone();
                }
            }
        }
        let kind = Kind::JetStream {
            stream: spec.stream.to_owned(),
        };
        let parked = group
            .as_ref()
            .and_then(|group| state.parked.remove(group))
            .unwrap_or_default();
        let (feed, sender) = self.register(&mut state, spec.filter, group, kind);
        for parked in parked {
            let _ = sender.send(BusDelivery {
                message: parked.message,
                sequence: parked.sequence,
                delivered: parked.delivered,
                release: Release::counted(self.coordinator()),
            });
        }
        drop(state);
        for (message, sequence) in backlog {
            let _ = sender.send(BusDelivery {
                message,
                sequence,
                delivered: 1,
                release: Release::counted(self.coordinator()),
            });
        }
        (feed, sender, consumer)
    }

    /// Removes a subscription, returning the group it belonged to; an id already gone is ignored.
    pub(crate) fn unsubscribe(&self, id: u64) -> Option<Group> {
        self.state()
            .subscriptions
            .remove(&id)
            .and_then(|subscription| subscription.group)
    }

    /// Hands a durable consumer's delivery to another subscription reading it, or holds it for the
    /// next one when none is open: the consumer outlives its subscriptions on a server, and so do
    /// its unacknowledged messages.
    pub(crate) fn reroute(&self, group: &Group, delivery: BusDelivery) {
        let mut state = self.state();
        if self.is_closed() {
            return;
        }
        let mut members: Vec<&DeliverySender> = state
            .subscriptions
            .values()
            .filter(|subscription| subscription.group.as_ref() == Some(group))
            .map(|subscription| &subscription.sender)
            .collect();
        let delivery = if members.is_empty() {
            Err(delivery)
        } else {
            let turn = state.turns.get(group).copied().unwrap_or(0);
            let picked = usize::try_from(turn % members.len() as u64).unwrap_or(0);
            members
                .swap_remove(picked)
                .send(delivery)
                .map_err(|failed| failed.0)
        };
        match delivery {
            Ok(()) => {
                let turn = state.turns.entry(group.clone()).or_insert(0);
                *turn = turn.wrapping_add(1);
            }
            Err(delivery) => state.parked.entry(group.clone()).or_default().push(Parked {
                message: delivery.message,
                sequence: delivery.sequence,
                delivered: delivery.delivered,
            }),
        }
    }

    /// The message as the client frames it, or the refusal the client or the server answers
    /// with: a subject the frame cannot carry, a header with no NATS form, a message over the
    /// server's payload limit.
    fn frame(
        subject: &str,
        payload: Bytes,
        headers: &HeaderMap,
        reply: Option<&str>,
    ) -> Result<async_nats::Message, NatsError> {
        check_publish(subject).map_err(|err| NatsError::Publish(Box::new(err)))?;
        let wire = headers_to_nats(headers)?;
        let header_length = if headers.is_empty() {
            0
        } else {
            // `NATS/1.0\r\n`, one `name: value\r\n` per header, and the closing `\r\n`.
            10 + headers
                .iter()
                .map(|(name, value)| name.len() + 2 + value.len() + 2)
                .sum::<usize>()
                + 2
        };
        let length = header_length + payload.len();
        if length > MAX_PAYLOAD {
            return Err(NatsError::Publish(
                format!("Payload size limit of {MAX_PAYLOAD} exceeded by message size of {length}")
                    .into(),
            ));
        }
        Ok(async_nats::Message {
            subject: subject.into(),
            reply: reply.map(Into::into),
            payload,
            headers: wire,
            status: None,
            description: None,
            length,
        })
    }

    /// Publishes a message: every matching subscription receives it as the server routes it,
    /// and a stream whose consumer reads the subject stores it.
    ///
    /// A plain publish that a stream refuses (a stated expectation that does not hold) still
    /// reaches the Core subscriptions and is simply not stored, as on a server, where nobody is
    /// told. A `JetStream` publish requires a stream and reports the refusal instead.
    ///
    /// # Errors
    ///
    /// [`NatsError::Closed`] after shutdown; [`NatsError::Publish`] for what the client refuses
    /// to frame; [`NatsError::JetStream`] for a `JetStream` publish no stream takes.
    pub(crate) fn publish(
        &self,
        subject: &str,
        payload: Bytes,
        headers: &HeaderMap,
        reply: Option<&str>,
        mode: PublishMode,
        origin: Origin,
    ) -> Result<Option<Stored>, NatsError> {
        self.ensure_live(subject)?;
        let message = Self::frame(subject, payload, headers, reply)?;
        let echo = origin == Origin::External || !self.settings.no_echo;

        let mut state = self.state();
        // Checked again under the lock: `close` marks the bus closed while holding it, so a
        // publish that passed the first check cannot land on a closed bus.
        self.ensure_live(subject)?;
        let capturing = state.capturing(subject);
        let expected = text(headers, EXPECTED_STREAM);
        let target = match (&expected, capturing) {
            (Some(expected), Some(index)) if state.streams[index].name != *expected => {
                Err("expected stream does not match".to_owned())
            }
            // A stream the bus knows serves the subjects its consumers read: naming it does not
            // make it take another one, and the server refuses such a publish. A stream no
            // consumer has named yet is taken at its word.
            (Some(expected), None) if state.knows(expected) => Ok(None),
            (Some(expected), None) => Ok(Some(expected.clone())),
            (_, Some(index)) => Ok(Some(state.streams[index].name.clone())),
            (None, None) => Ok(None),
        };
        let stored = match (target, mode) {
            (Ok(Some(stream)), _) => state.stream_mut(&stream).store(&message, headers),
            (Ok(None), PublishMode::Core) => Err(String::new()),
            (Ok(None), PublishMode::JetStream) => {
                Err("no stream found for given subject".to_owned())
            }
            (Err(refusal), _) => Err(refusal),
        };
        let stored = match (stored, mode) {
            (Ok(stored), _) => Some(stored),
            (Err(refusal), PublishMode::JetStream) => {
                return Err(NatsError::JetStream(refusal.into()));
            }
            (Err(_), PublishMode::Core) => None,
        };
        state.log.entry(subject.to_owned()).or_default().push(
            RawMessage::new(subject.to_owned(), message.payload.clone())
                .with_headers(headers.clone()),
        );
        let reaching = stored
            .as_ref()
            .filter(|stored| !stored.duplicate)
            .map(|stored| stored.stream.as_str());
        let recipients = state.recipients(subject, reaching, echo);

        // The sends stay under the lock, so the counters `close` returns include this publish,
        // and a durable subscription closing concurrently finds the delivery in its channel.
        if origin == Origin::Connection {
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
        let sequence = stored.as_ref().map_or(0, |stored| stored.sequence);
        for (sender, kind) in recipients {
            let (sequence, release) = match kind {
                Kind::Core => (0, Release::counted(self.coordinator())),
                Kind::Inbox => (0, Release::uncounted()),
                Kind::JetStream { .. } => (sequence, Release::counted(self.coordinator())),
            };
            if sender
                .send(BusDelivery {
                    message: message.clone(),
                    sequence,
                    delivered: 1,
                    release,
                })
                .is_ok()
            {
                self.received.fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(state);
        Ok(stored)
    }

    /// Opens a request's inbox, or answers "no responders" when nothing would answer: no Core
    /// subscription this connection's publish reaches, and no stream that stores the subject.
    fn open_inbox(
        self: &Arc<Self>,
        subject: &str,
        inbox: SubjectPattern,
    ) -> Result<Feed, NatsError> {
        let mut state = self.state();
        let responders = !self.settings.no_echo
            && state.subscriptions.values().any(|subscription| {
                subscription.kind == Kind::Core && subscription.pattern.matches(subject)
            });
        if !responders && state.capturing(subject).is_none() {
            return Err(NatsError::Publish("no responders".into()));
        }
        let feed = self.register(&mut state, inbox, None, Kind::Inbox).0;
        drop(state);
        Ok(feed)
    }

    /// A request: the message goes out with a fresh inbox as its reply subject, and the first
    /// message on the inbox is the answer.
    ///
    /// # Errors
    ///
    /// What [`publish`](Self::publish) refuses; [`NatsError::Publish`] at once when nothing is
    /// subscribed to the subject (the server's "no responders"); [`NatsError::RequestTimeout`]
    /// when no answer arrives within `timeout`.
    pub(crate) async fn request(
        self: &Arc<Self>,
        subject: &str,
        payload: Bytes,
        headers: &HeaderMap,
        timeout: Duration,
    ) -> Result<async_nats::Message, NatsError> {
        self.ensure_live(subject)?;
        let inbox = format!("{}.{}", self.settings.inbox_prefix, self.next_id());
        let pattern =
            SubjectPattern::parse(&inbox).map_err(|err| NatsError::Publish(Box::new(err)))?;
        let mut feed = self.open_inbox(subject, pattern)?;
        let stored = self.publish(
            subject,
            payload,
            headers,
            Some(&inbox),
            PublishMode::Core,
            Origin::Connection,
        )?;
        // A stream that stored the request answers it with its acknowledgement.
        if let Some(stored) = stored {
            let ack = format!(
                r#"{{"stream":"{}","seq":{}{}}}"#,
                stored.stream,
                stored.sequence,
                if stored.duplicate {
                    r#","duplicate":true"#
                } else {
                    ""
                },
            );
            self.publish(
                &inbox,
                Bytes::from(ack),
                &HeaderMap::new(),
                None,
                PublishMode::Core,
                Origin::External,
            )?;
        }
        match tokio::time::timeout(timeout, feed.recv()).await {
            Ok(Some(delivery)) => Ok(delivery.message),
            Ok(None) => Err(NatsError::Closed {
                subject: subject.to_owned(),
            }),
            Err(_) => Err(NatsError::RequestTimeout),
        }
    }

    /// Every message published to `subject`, in publish order.
    pub(crate) fn published(&self, subject: &str) -> Vec<RawMessage> {
        self.state().log.get(subject).cloned().unwrap_or_default()
    }
}
