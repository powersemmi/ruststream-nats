//! NATS routing as the test harness asks for it: which of a broker's subscriptions a publish to
//! one subject reaches.
//!
//! The connected broker records every subscription it opens, in process and live alike, and
//! [`RouteBook::routes`] answers from that record by the server's rules:
//!
//! * a Core subscription receives every message whose subject its subject matches, `*` standing
//!   for one token and `>` for the rest;
//! * a queue group, named alike over any subjects, delivers each message to one member;
//! * a `JetStream` consumer receives what its stream stores and its filter subject matches. A
//!   subject is stored by the first stream a consumer's filter places it in (streams do not
//!   overlap on a server), and a durable consumer shared by several subscriptions delivers to one
//!   of them;
//! * with `no_echo` set on the connection, a Core subscription receives nothing this connection
//!   publishes.
//!
//! Where a group delivers to one member, the answer is its first subscription: the server's pick
//! is not observable before the delivery.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use super::bus::Group;
use super::subject::{SubjectPattern, check_publish};

/// One subscription the broker opened.
#[derive(Debug)]
pub(crate) struct Route {
    /// The name the app subscribed under: the descriptor's subject.
    name: String,
    /// What the subscription reads: the subject, or the consumer's filter.
    filter: SubjectPattern,
    group: Option<Group>,
    /// The stream a `JetStream` consumer reads.
    stream: Option<String>,
}

impl Route {
    pub(crate) fn core(name: &str, filter: SubjectPattern, queue: Option<&str>) -> Self {
        Self {
            name: name.to_owned(),
            filter,
            group: queue.map(|queue| Group::Queue(queue.to_owned())),
            stream: None,
        }
    }

    pub(crate) fn jetstream(
        name: &str,
        filter: SubjectPattern,
        stream: &str,
        durable: Option<&str>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            filter,
            group: durable.map(|durable| Group::Durable {
                stream: stream.to_owned(),
                name: durable.to_owned(),
            }),
            stream: Some(stream.to_owned()),
        }
    }
}

/// The subscriptions one connected broker opened, in the order it opened them.
#[derive(Debug)]
pub(crate) struct RouteBook {
    no_echo: bool,
    routes: Mutex<Vec<Route>>,
}

impl RouteBook {
    pub(crate) const fn new(no_echo: bool) -> Self {
        Self {
            no_echo,
            routes: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn record(&self, route: Route) {
        self.routes
            .lock()
            .expect("route book mutex poisoned")
            .push(route);
    }

    /// The positions in `subscriptions` a publish to `destination` from this connection reaches.
    ///
    /// `subscriptions` names the app's subscriptions in the order they opened; a name that occurs
    /// more than once is matched to the recorded subscriptions of that name in the same order.
    pub(crate) fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        if check_publish(destination).is_err() {
            return Vec::new();
        }
        let routes = self.routes.lock().expect("route book mutex poisoned");
        let stored_in = routes
            .iter()
            .find(|route| route.stream.is_some() && route.filter.matches(destination))
            .and_then(|route| route.stream.as_deref());
        let mut seen: HashMap<&str, usize> = HashMap::new();
        let mut groups: HashSet<&Group> = HashSet::new();
        let mut reached = Vec::new();
        for (position, name) in subscriptions.iter().enumerate() {
            let occurrence = seen.entry(name).or_insert(0);
            let route = routes
                .iter()
                .filter(|route| route.name == *name)
                .nth(*occurrence);
            *occurrence += 1;
            let Some(route) = route else {
                // A subscription the broker did not open here: read its name as a subject.
                if SubjectPattern::parse(name).is_ok_and(|filter| filter.matches(destination)) {
                    reached.push(position);
                }
                continue;
            };
            let delivers = route
                .stream
                .as_ref()
                .map_or(!self.no_echo, |stream| stored_in == Some(stream.as_str()));
            if !delivers || !route.filter.matches(destination) {
                continue;
            }
            if let Some(group) = &route.group
                && !groups.insert(group)
            {
                continue;
            }
            reached.push(position);
        }
        drop(routes);
        reached
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(subject: &str) -> SubjectPattern {
        SubjectPattern::parse(subject).expect("pattern parses")
    }

    fn book(routes: Vec<Route>) -> RouteBook {
        let book = RouteBook::new(false);
        for route in routes {
            book.record(route);
        }
        book
    }

    #[test]
    fn every_matching_subscription_receives_a_publish() {
        let book = book(vec![
            Route::core("orders.created", pattern("orders.created"), None),
            Route::core("orders.*", pattern("orders.*"), None),
            Route::core("orders.>", pattern("orders.>"), None),
            Route::core("payments", pattern("payments"), None),
        ]);
        let names = ["orders.created", "orders.*", "orders.>", "payments"];

        assert_eq!(book.routes("orders.created", &names), [0, 1, 2]);
        assert_eq!(book.routes("orders.eu.created", &names), [2]);
        assert_eq!(book.routes("payments", &names), [3]);
    }

    #[test]
    fn a_queue_group_delivers_to_one_member_across_subjects() {
        let book = book(vec![
            Route::core("orders.*", pattern("orders.*"), Some("workers")),
            Route::core("orders.>", pattern("orders.>"), Some("workers")),
            Route::core("orders.*", pattern("orders.*"), Some("audit")),
        ]);
        let names = ["orders.*", "orders.>", "orders.*"];

        assert_eq!(book.routes("orders.created", &names), [0, 2]);
        assert_eq!(book.routes("orders.eu.created", &names), [1]);
    }

    #[test]
    fn a_consumer_receives_what_its_stream_stores_under_its_filter() {
        let book = book(vec![
            Route::jetstream(
                "orders",
                pattern("orders.created"),
                "ORDERS",
                Some("worker"),
            ),
            Route::jetstream(
                "orders",
                pattern("orders.created"),
                "ORDERS",
                Some("worker"),
            ),
            Route::jetstream("audit", pattern("orders.>"), "ORDERS", None),
        ]);
        let names = ["orders", "orders", "audit"];

        assert_eq!(book.routes("orders.created", &names), [0, 2]);
        assert_eq!(book.routes("orders.cancelled", &names), [2]);
        assert!(book.routes("payments", &names).is_empty());
    }

    #[test]
    fn a_connection_without_echo_reaches_no_core_subscription_of_its_own() {
        let book = RouteBook::new(true);
        book.record(Route::core("orders", pattern("orders"), None));
        book.record(Route::jetstream(
            "stored",
            pattern("orders"),
            "ORDERS",
            None,
        ));

        assert_eq!(book.routes("orders", &["orders", "stored"]), [1]);
    }
}
