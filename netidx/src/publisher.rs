use crate::{
    path::Path,
    pool::Pool,
    protocol::*,
    socket_channel::{self, Channel, ReadChannel, WriteChannel},
    try_continue,
};
use anyhow::{anyhow, bail, Error, Result};
use bytes::Bytes;
use futures::{
    channel::{
        mpsc::{Receiver, Sender, UnboundedSender},
        oneshot,
    },
    future,
    prelude::*,
    select_biased,
    stream::FuturesUnordered,
};
use fxhash::{FxHashMap, FxHashSet};
use lazy_static::lazy_static;
use log::{debug, error, info};
use pack::Pack;
use parking_lot::Mutex;
use pool::Pooled;
use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap, HashSet},
    iter, mem,
    net::SocketAddr,
    pin::Pin,
    result,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task, time,
};

lazy_static! {
    static ref UPDATES: Pool<Vec<ToSubscriber>> = Pool::new(100, 10_000);
    static ref RAWUNSUBS: Pool<Vec<(ClId, Id)>> = Pool::new(100, 10_000);

    // estokes 2021: This is reasonable because there will never be
    // that many publishers in a process. Since a publisher wraps
    // actual expensive OS resources, users will hit other constraints
    // (e.g. file descriptor exhaustion) long before N^2 drop becomes
    // a problem.
    //
    // Moreover publisher is architected such that there should not be
    // very many applications that require more than one publisher in
    // a process.
    //
    // The payback on the other hand is saving a word in EVERY
    // published value, and that adds up quite fast to a lot of saved
    // memory.
    static ref PUBLISHERS: Mutex<Vec<PublisherWeak>> = Mutex::new(Vec::new());
}

const MAGIC: u64 = 4;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

/// This represents a published value. When it is dropped the value
/// will be unpublished.
pub struct Val(Id);

impl Drop for Val {
    fn drop(&mut self) {
        PUBLISHERS.lock().retain(|t| match t.upgrade() {
            None => false,
            Some(t) => {
                t.0.lock().destroy_val(self.0);
                true
            }
        })
    }
}

impl Val {
    /// Queue an update to the published value in the specified
    /// batch. Multiple updates to multiple Vals can be queued in a
    /// batch before it is committed, and updates may be concurrently
    /// queued in different batches. Queuing updates in a batch has no
    /// effect until the batch is committed. On commit, subscribers
    /// will receive all the queued values in the batch in the order
    /// they were queued. If multiple batches have queued values, then
    /// subscribers will receive the queued values in the order the
    /// batches are committed.
    ///
    /// Clients that subscribe after an update is queued, but before
    /// the batch is committed will still receive the update.
    pub fn update<T: Pack>(&self, batch: &mut UpdateBatch, v: T) {
        batch.updates.push(BatchMsg::Update(None, self.0, v.into()))
    }

    /// Same as update, except the argument can be TryInto<Value>
    /// instead of Into<Value>
    pub fn try_update<T: TryInto<Value>>(
        &self,
        batch: &mut UpdateBatch,
        v: T,
    ) -> result::Result<(), T::Error> {
        Ok(batch.updates.push(BatchMsg::Update(None, self.0, v.try_into()?)))
    }

    /// update the current value only if the new value is different
    /// from the existing one. Otherwise exactly the same as update.
    pub fn update_changed<T: Pack>(&self, batch: &mut UpdateBatch, v: T) {
        batch.updates.push(BatchMsg::UpdateChanged(self.0, v.into()))
    }

    /// Queue sending `v` as an update ONLY to the specified
    /// subscriber, and do not update `current`.
    pub fn update_subscriber<T: Pack>(&self, batch: &mut UpdateBatch, dst: ClId, v: T) {
        batch.updates.push(BatchMsg::Update(Some(dst), self.0, v.into()));
    }

    /// Same as update_subscriber except the argument can be TryInto<Value>.
    pub fn try_update_subscriber<T: Pack>(
        &self,
        batch: &mut UpdateBatch,
        dst: ClId,
        v: T,
    ) -> result::Result<(), T::Error> {
        Ok(batch.updates.push(BatchMsg::Update(Some(dst), self.0, v.try_into()?)))
    }

    /// Queue unsubscribing the specified client. Like update, this
    /// will only take effect when the specified batch is committed.
    pub fn unsubscribe(&self, batch: &mut UpdateBatch, dst: ClId) {
        match &mut batch.unsubscribes {
            Some(u) => u.push((dst, self.0)),
            None => {
                let mut u = RAWUNSUBS.take();
                u.push((dst, self.0));
                batch.unsubscribes = Some(u);
            }
        }
    }

    /// Get the unique `Id` of this `Val`
    pub fn id(&self) -> Id {
        self.0
    }
}

/// Publish values. Publisher is internally wrapped in an Arc, so
/// cloning it is virtually free. When all references to to the
/// publisher have been dropped the publisher will shutdown the
/// listener, and remove all published paths from the resolver server.
#[derive(Debug, Clone)]
pub struct Publisher(Arc<Mutex<PublisherInner>>);

impl Publisher {
    fn downgrade(&self) -> PublisherWeak {
        PublisherWeak(Arc::downgrade(&self.0))
    }

    pub async fn new(bind: &str, slack: usize) -> Result<Self> {
        // TODO
    }

    /// Publish `Path` with initial value `init` and flags `flags`. It
    /// is an error for the same publisher to publish the same path
    /// twice, however different publishers may publish a given path
    /// as many times as they like. Subscribers will then pick
    /// randomly among the advertised publishers when subscribing. See
    /// `subscriber`
    ///
    /// If specified the write channel will be registered before the
    /// value is published, so there can be no race (however small)
    /// that might cause you to miss a write.
    pub fn publish_with_flags_and_writes<T>(&self, path: Path, init: T) -> Result<Val>
    where
        T: Pack,
    {
        let init: Value = init.try_into()?;
        let id = Id::new();
        let destroy_on_idle = flags.contains(PublishFlags::DESTROY_ON_IDLE);
        flags.remove(PublishFlags::DESTROY_ON_IDLE);
        let mut pb = self.0.lock();
        pb.check_publish(&path)?;
        let subscribed = pb
            .hc_subscribed
            .entry(BTreeSet::new())
            .or_insert_with(|| Arc::new(HashSet::default()))
            .clone();
        pb.by_id.insert(
            id,
            Published { current: init, subscribed, path: path.clone(), aliases: None },
        );
        if destroy_on_idle {
            pb.destroy_on_idle.insert(id);
        }
        if let Some(tx) = tx {
            pb.writes(id, tx);
        }
        pb.publish(id, flags, path.clone());
        Ok(Val(id))
    }

    /// get the `SocketAddr` that publisher is bound to
    pub fn addr(&self) -> SocketAddr {
        self.0.lock().addr
    }

    /// Retrieve the Id of path if it is published
    pub fn id<S: AsRef<str>>(&self, path: S) -> Option<Id> {
        self.0.lock().by_path.get(path.as_ref()).map(|id| *id)
    }

    /// Get the `Path` of a published value.
    pub fn path(&self, id: Id) -> Option<Path> {
        self.0.lock().by_id.get(&id).map(|pbl| pbl.path.clone())
    }

    /// Return all the aliases for the specified `id`
    pub fn aliases(&self, id: Id) -> Vec<Path> {
        let pb = self.0.lock();
        match pb.by_id.get(&id) {
            None => vec![],
            Some(pbv) => match &pbv.aliases {
                None => vec![],
                Some(al) => al.iter().cloned().collect(),
            },
        }
    }

    // TODO: return ref instead
    /// Get a copy of the current value of a published `Val`
    pub fn current(&self, id: &Id) -> Option<Bytes> {
        self.0.lock().by_id.get(&id).map(|p| p.current.clone())
    }

    /// Get a list of clients subscribed to a published `Val`
    pub fn subscribed(&self, id: &Id) -> Vec<ClId> {
        self.0
            .lock()
            .by_id
            .get(&id)
            .map(|p| p.subscribed.iter().copied().collect::<Vec<_>>())
            .unwrap_or_else(Vec::new)
    }

    /// Put the list of clients subscribed to a published `Val` into
    /// the specified collection.
    pub fn put_subscribed(&self, id: &Id, into: &mut impl Extend<ClId>) {
        if let Some(p) = self.0.lock().by_id.get(&id) {
            into.extend(p.subscribed.iter().copied())
        }
    }

    /// Return true if the specified client is subscribed to the
    /// specifed Id.
    pub fn is_subscribed(&self, id: &Id, client: &ClId) -> bool {
        match self.0.lock().by_id.get(&id) {
            Some(p) => p.subscribed.contains(client),
            None => false,
        }
    }

    /// Return the auth information associated with the specified
    /// client id. If the authentication mechanism is Krb5 then this
    /// will be the remote user's user principal name,
    /// e.g. eric@RYU-OH.ORG on posix systems. If the auth mechanism
    /// is tls, then this will be the common name of the user's
    /// certificate. If the authentication mechanism is Local then
    /// this will be the local user name.
    ///
    /// This will always be None if the auth mechanism is Anonymous.
    pub fn auth_info(&self, client: &ClId) -> Option<AuthInfo> {
        self.0.lock().clients.get(client).and_then(|c| c.auth.clone())
    }

    /// Get the number of clients subscribed to a published `Val`
    pub fn subscribed_len(&self, id: &Id) -> usize {
        self.0.lock().by_id.get(&id).map(|p| p.subscribed.len()).unwrap_or(0)
    }
}

#[derive(Clone)]
struct PublisherWeak(Weak<Mutex<PublisherInner>>);

impl PublisherWeak {
    fn upgrade(&self) -> Option<Publisher> {
        Weak::upgrade(&self.0).map(|r| Publisher(r))
    }
}

#[derive(Debug)]
struct PublisherInner {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    clients: FxHashMap<ClId, PublisherClient>,
    hc_subscribed: FxHashMap<BTreeSet<ClId>, Subscribed>,
    by_path: HashMap<Path, Id>,
    by_id: FxHashMap<Id, Published>,
    advertised: HashMap<Path, HashSet<Path>>,
    to_publish: Pooled<HashSet<Path>>,
    to_unpublish: Pooled<HashSet<Path>>,
    publish_triggered: bool,
    trigger_publish: UnboundedSender<Option<oneshot::Sender<()>>>,
}

impl PublisherInner {
    fn cleanup(&mut self) -> bool {
        match mem::replace(&mut self.stop, None) {
            None => false,
            Some(stop) => {
                let _ = stop.send(());
                self.clients.clear();
                self.by_id.clear();
                true
            }
        }
    }

    fn is_advertised(&self, path: &Path) -> bool {
        self.advertised
            .iter()
            .any(|(b, set)| Path::is_parent(&**b, &**path) && set.contains(path))
    }

    pub fn check_publish(&self, path: &Path) -> Result<()> {
        if !Path::is_absolute(&path) {
            bail!("can't publish a relative path")
        }
        if self.stop.is_none() {
            bail!("publisher is dead")
        }
        if self.by_path.contains_key(path) {
            bail!("already published")
        }
        Ok(())
    }

    pub fn publish(&mut self, id: Id, path: Path) {
        self.by_path.insert(path.clone(), id);
        self.to_unpublish.remove(&path);
        self.to_publish.insert(path.clone());
        self.trigger_publish();
    }

    fn unpublish(&mut self, path: &Path) {
        self.by_path.remove(path);
        if !self.is_advertised(path) {
            self.to_publish.remove(path);
            self.to_unpublish.insert(path.clone());
            self.trigger_publish();
        }
    }

    fn trigger_publish(&mut self) {
        if !self.publish_triggered {
            self.publish_triggered = true;
            let _: Result<_, _> = self.trigger_publish.unbounded_send(None);
        }
    }

    fn subscribe(
        &mut self,
        con: &mut WriteChannel,
        client: ClId,
        path: Path,
    ) -> Result<()> {
        if let Some(id) = self.by_path.get(&path) {
            let id = *id;
            if let Some(ut) = self.by_id.get_mut(&id) {
                if let Some(cl) = self.clients.get_mut(&client) {
                    cl.subscribed.insert(id);
                }
                let subs = BTreeSet::from_iter(
                    iter::once(client).chain(ut.subscribed.iter().copied()),
                );
                match self.hc_subscribed.entry(subs) {
                    Entry::Occupied(e) => {
                        ut.subscribed = Arc::clone(e.get());
                    }
                    Entry::Vacant(e) => {
                        let mut s = HashSet::clone(&ut.subscribed);
                        s.insert(client);
                        ut.subscribed = Arc::new(s);
                        e.insert(Arc::clone(&ut.subscribed));
                    }
                }
                // TODO: don't clone via ToSubscriber<'a>
                let m = ToSubscriber::Subscribed(path, id, ut.current.clone());
                con.queue_send(&m)?;
                // t.send_event(Event::Subscribe(id, client));
            }
            // TODO: missing a NoSuchValue case?
        } else {
            // TODO: need to clone?
            con.queue_send(&ToSubscriber::NoSuchValue(path.clone()))?;
        }
        Ok(())
    }

    fn unsubscribe(&mut self, client: ClId, id: Id) {
        if let Some(ut) = self.by_id.get_mut(&id) {
            let current_subs = BTreeSet::from_iter(ut.subscribed.iter().copied());
            let new_subs = BTreeSet::from_iter(
                ut.subscribed.iter().filter(|a| *a != &client).copied(),
            );
            match self.hc_subscribed.entry(new_subs) {
                Entry::Occupied(e) => {
                    ut.subscribed = e.get().clone();
                }
                Entry::Vacant(e) => {
                    let mut h = HashSet::clone(&ut.subscribed);
                    h.remove(&client);
                    ut.subscribed = Arc::new(h);
                    e.insert(Arc::clone(&ut.subscribed));
                }
            }
            if let Entry::Occupied(e) = self.hc_subscribed.entry(current_subs) {
                if Arc::strong_count(e.get()) == 1 {
                    e.remove();
                }
            }
            if let Some(cl) = self.clients.get_mut(&client) {
                cl.subscribed.remove(&id);
            }
            // t.send_event(Event::Unsubscribe(id, client));
        }
    }
}

#[derive(Debug)]
struct PublisherClient {
    msg_queue: MsgQ,
    subscribed: FxHashSet<Id>,
    auth: Option<AuthInfo>,
}

#[derive(Debug)]
pub struct Published {
    current: Bytes,
    subscribed: Subscribed,
    path: Path,
    aliases: Option<Box<FxHashSet<Path>>>,
}

impl Published {
    pub fn current(&self) -> &Bytes {
        &self.current
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn subscribed(&self) -> &FxHashSet<ClId> {
        &self.subscribed
    }
}

type MsgQ = Sender<(Option<Duration>, Update)>;

// The set of clients subscribed to a given value is hashconsed.
// Instead of having a seperate hash table for each published value,
// we can just keep a pointer to a set shared by other published
// values. Since in the case where we care about memory usage there
// are many more published values than clients we can save a lot of
// memory this way. Roughly the size of a hashmap, plus it's
// keys/values, replaced by 1 word.
type Subscribed = Arc<FxHashSet<ClId>>;

#[derive(Debug)]
struct Update {
    updates: Pooled<Vec<ToSubscriber>>,
    unsubscribes: Option<Pooled<Vec<Id>>>,
}

impl Update {
    fn new() -> Self {
        Self { updates: UPDATES.take(), unsubscribes: None }
    }
}

pub async fn run(bind: &str) -> Result<()> {
    let listener = TcpListener::bind(bind).await?;
    loop {
        match listener.accept().await {
            Err(e) => error!("failed to accept connection: {e:?}"),
            Ok((s, addr)) => {
                debug!("accepted client: {addr:?}");
                try_continue!("set nodelay", s.set_nodelay(true));
            }
        }
    }
    #[allow(unreachable_code)]
    Ok(())
}

struct ClientCtx {
    publisher: PublisherWeak,
    client: ClId,
    batch: Vec<ToPublisher>,
    flushing_updates: bool,
    flush_timeout: Option<Duration>,
}

impl ClientCtx {
    async fn hello(&mut self, mut con: TcpStream) -> Result<Channel> {
        // static NO: &str = "authentication mechanism not supported";
        debug!("hello_client");
        socket_channel::write_raw(&mut con, &MAGIC).await?;
        if socket_channel::read_raw::<u64, _>(&mut con).await? != MAGIC {
            bail!("incompatible protocol version")
        }
        let hello: ClientHello = socket_channel::read_raw(&mut con).await?;
        debug!("hello_client received {:?}", hello);
        match hello.auth {
            AuthMethod::Anonymous => Ok(Channel::new::<TcpStream>(con)),
            AuthMethod::Local { id } => {
                self.set_user(AuthInfo { id });
                Ok(Channel::new::<TcpStream>(con))
            }
        }
    }

    fn set_user(&mut self, auth: AuthInfo) {
        if let Some(pb) = self.publisher.upgrade() {
            let mut t = pb.0.lock();
            if let Some(ci) = t.clients.get_mut(&self.client) {
                ci.auth = Some(auth);
            }
        }
    }

    fn handle_batch(&mut self, con: &mut WriteChannel) -> Result<()> {
        let t_st = self.publisher.upgrade().ok_or_else(|| anyhow!("dead publisher"))?;
        let mut pb = t_st.0.lock();
        let mut gc = false;
        for msg in self.batch.drain(..) {
            match msg {
                ToPublisher::Heartbeat => {}
                ToPublisher::Subscribe { path } => {
                    gc = true;
                    pb.subscribe(con, self.client, path)?;
                }
                ToPublisher::Unsubscribe(id) => {
                    gc = true;
                    pb.unsubscribe(self.client, id);
                    con.queue_send(&ToSubscriber::Unsubscribed(id))?;
                }
            }
        }
        if gc {
            pb.hc_subscribed.retain(|_, v| Arc::get_mut(v).is_none());
        }
        Ok(())
    }

    pub async fn run(
        mut self,
        con: TcpStream,
        mut updates: Receiver<(Option<Duration>, Update)>,
    ) -> Result<()> {
        async fn flush(c: &mut WriteChannel, timeout: Option<Duration>) -> Result<()> {
            if c.bytes_queued() > 0 {
                if let Some(timeout) = timeout {
                    c.flush_timeout(timeout).await
                } else {
                    c.flush().await
                }
            } else {
                future::pending().await
            }
        }
        // async fn read_updates(
        //     flushing: bool,
        //     c: &mut Receiver<(Option<Duration>, Update)>,
        // ) -> Option<(Option<Duration>, Update)> {
        //     if flushing {
        //         future::pending().await
        //     } else {
        //         c.next().await
        //     }
        // }
        let mut hb = tokio::time::interval(HEARTBEAT_INTERVAL);
        let (mut read_con, mut write_con) =
            time::timeout(HELLO_TIMEOUT, self.hello(con)).await??.split();
        loop {
            select_biased! {
                r = flush(&mut write_con, self.flush_timeout).fuse() => {
                    r?;
                    self.flushing_updates = false;
                    self.flush_timeout = None;
                },
                _ = hb.tick().fuse() => {
                    write_con.queue_send(&ToSubscriber::Heartbeat)?;
                },
                // s = self.deferred_subs.next() =>
                //     self.handle_deferred_sub(&mut write_con, s)?,
                r = read_con.receive_batch(&mut self.batch).fuse() => {
                    r?;
                    self.handle_batch(&mut write_con)?;
                }
                // u = read_updates(self.flushing_updates, &mut updates).fuse() => {
                //     match u {
                //         None => break Ok(()),
                //         Some(u) => self.handle_updates(&mut write_con, u)?,
                //     }
                // },
            }
        }
    }
}
