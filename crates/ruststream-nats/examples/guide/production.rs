//! The service of the production guide: an orders service that connects to a NATS cluster over TLS
//! with a `.creds` file, with its servers and file paths read from the environment.
//!
//! ```text
//! NATS_SERVERS=tls://nats-1.example.com:4222,tls://nats-2.example.com:4222 \
//! NATS_CREDS=/etc/nats/orders.creds NATS_CA=/etc/nats/ca.pem \
//!     cargo run --example guide_production --features testing -- run
//! ```

use std::path::PathBuf;
use std::time::Duration;
use std::{env, fs, io};

use async_nats::ConnectOptions;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:broker]
/// Where the cluster is and who the service is, as the deployment provides them.
struct Deployment {
    /// One server address or a comma-separated list.
    servers: String,
    /// The contents of the service's `.creds` file: its user JWT and its `NKey` seed.
    creds: String,
    /// The certificate authority that signed the servers' certificates.
    ca: PathBuf,
}

impl Deployment {
    fn from_env() -> io::Result<Self> {
        let var = |name: &str| {
            env::var(name).map_err(|_| io::Error::new(io::ErrorKind::NotFound, name.to_owned()))
        };
        Ok(Self {
            servers: var("NATS_SERVERS")?,
            creds: fs::read_to_string(var("NATS_CREDS")?)?,
            ca: var("NATS_CA")?.into(),
        })
    }
}

fn broker(deployment: Deployment) -> io::Result<NatsBroker> {
    let options = ConnectOptions::with_credentials(&deployment.creds)?
        .add_root_certificates(deployment.ca)
        .require_tls(true)
        .name("orders")
        .connection_timeout(Duration::from_secs(10));
    Ok(NatsBroker::new(deployment.servers).with_options(options))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.placed")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct Confirmation {
    order: u64,
}

// --8<-- [start:service]
#[subscriber(
    JetStreamSubject::new("orders.placed", "ORDERS").durable("confirmer"),
    reply
)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order.id, "order confirmed");
    Confirmation { order: order.id }
}

fn service(broker: NatsBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm);
    })
}

#[ruststream::app]
fn app() -> impl App {
    let broker = Deployment::from_env()
        .and_then(broker)
        .expect("NATS_SERVERS, NATS_CREDS and NATS_CA describe the connection");
    service(broker)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Confirmation, Deployment, Order, broker, service};

    /// A `.creds` file as `nsc` writes it, for an account that exists nowhere.
    const CREDS: &str = "-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.e30.c2lnbmF0dXJl
------END NATS USER JWT------

-----BEGIN USER NKEY SEED-----
SUAIO3FHUX5PNV2LQIIP7TZ3N4L7TX3W53MQGEIVYFIGA635OZCKEYHFLM
------END USER NKEY SEED------
";

    fn deployment(creds: &str) -> Deployment {
        Deployment {
            servers: "tls://nats-1.example.com:4222,tls://nats-2.example.com:4222".to_owned(),
            creds: creds.to_owned(),
            // The client reads the certificate when it connects, which a test in process never
            // does, so the file need not exist.
            ca: "/etc/nats/ca.pem".into(),
        }
    }

    #[tokio::test]
    async fn the_production_configuration_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker(deployment(CREDS))?)).await?;

        tb.broker::<NatsBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Confirmation>("orders.confirmed")
            .assert_called_once()
            .with(&Confirmation { order: 1 });
        tb.shutdown().await?;
        Ok(())
    }

    #[test]
    fn a_creds_file_that_does_not_parse_is_refused() {
        let refused = broker(deployment("not a creds file")).err();

        assert_eq!(
            refused.map(|err| err.kind()),
            Some(io::ErrorKind::InvalidData),
        );
    }
}
