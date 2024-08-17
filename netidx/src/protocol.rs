use crate::{atomic_id, pack::Pack, path::Path};
use arcstr::ArcStr;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

atomic_id!(Id);
atomic_id!(ClId);

#[derive(Clone, Debug)]
pub struct AuthInfo {
    pub(crate) id: ArcStr,
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Pack, Serialize, Deserialize,
)]
pub enum AuthMethod {
    Anonymous,
    /// Local sockets, valid for clients on the same machine as the server;
    /// any self-identification is accepted as valid.
    Local {
        id: ArcStr,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Pack, Serialize, Deserialize)]
pub struct ClientHello {
    pub auth: AuthMethod,
}

#[derive(Debug, Clone, PartialEq, Pack, Serialize, Deserialize)]
pub enum ToPublisher {
    Heartbeat,
    /// Subscribe to the specified value, if it is not available
    /// the result will be NoSuchValue.
    Subscribe {
        path: Path,
    },
    /// Unsubscribe from the specified value, this will always result
    /// in an Unsubscribed message even if you weren't ever subscribed
    /// to the value, or it doesn't exist.
    Unsubscribe(Id),
    // Request,
    // Response,
}

#[derive(Debug, Clone, PartialEq, Pack)]
pub enum ToSubscriber {
    /// Indicates that the publisher is idle, but still
    /// functioning correctly.
    Heartbeat,
    /// The requested subscription to Path cannot be completed because
    /// it doesn't exist
    NoSuchValue(Path),
    /// Permission to subscribe to the specified path is denied.
    Denied(Path),
    /// You have been unsubscribed from Path. This can be the result
    /// of an Unsubscribe message, or it may be sent unsolicited, in
    /// the case the value is no longer published, or the publisher is
    /// in the process of shutting down.
    Unsubscribed(Id),
    // TODO: why is this not Option<Bytes>
    /// You are now subscribed to Path with subscription id `Id`, and
    /// The next message contains the first value for Id. All further
    /// communications about this subscription will only refer to the
    /// Id.
    Subscribed(Path, Id, Bytes),
    /// A value update to Id
    Update(Id, Bytes),
}

// TODO: i like client/server distinction because its clear whose message what
// but it would be nice to establish a conn then reverse the directionality, for
// certain use cases. should provide a mechanism for that

#[derive(Debug, Clone, Copy)]
pub enum Event {
    Destroyed(Id),
    Subscribe(Id, ClId),
    Unsubscribe(Id, ClId),
}
