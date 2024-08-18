use crate::{
    path::Path,
    pool::Pool,
    protocol::*,
    socket_channel::{self, Channel, ReadChannel, WriteChannel},
    try_continue, utils,
};
use anyhow::{anyhow, bail, Error, Result};
use bytes::Bytes;
use futures::{
    channel::{
        mpsc::{channel, unbounded, Receiver, Sender, UnboundedSender},
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
    static ref TOPUB: Pool<HashSet<Path>> = Pool::new(10, 10_000);
    static ref TOUPUB: Pool<HashSet<Path>> = Pool::new(5, 10_000);
    static ref TOUSUB: Pool<HashMap<Id, Subscribed>> = Pool::new(5, 10_000);
    static ref RAWBATCH: Pool<Vec<BatchMsg>> = Pool::new(100, 10_000);
    static ref UPDATES: Pool<Vec<ToSubscriber>> = Pool::new(100, 10_000);
    static ref RAWUNSUBS: Pool<Vec<(ClId, Id)>> = Pool::new(100, 10_000);
    static ref UNSUBS: Pool<Vec<Id>> = Pool::new(100, 10_000);
    static ref BATCH: Pool<FxHashMap<ClId, Update>> = Pool::new(100, 1000);

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
    pub fn update<T: Pack>(&self, batch: &mut UpdateBatch, v: &T) -> Result<()> {
        let buf = utils::pack(v)?.freeze();
        batch.updates.push(BatchMsg::Update(None, self.0, buf));
        Ok(())
    }

    /// Queue sending `v` as an update ONLY to the specified
    /// subscriber, and do not update `current`.
    pub fn update_subscriber<T: Pack>(
        &self,
        batch: &mut UpdateBatch,
        dst: ClId,
        v: &T,
    ) -> Result<()> {
        let buf = utils::pack(v)?.freeze();
        batch.updates.push(BatchMsg::Update(Some(dst), self.0, buf));
        Ok(())
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

#[derive(Debug, Clone)]
pub enum BatchMsg {
    Update(Option<ClId>, Id, Bytes),
}

/// A batch of updates to Vals
#[must_use = "update batches do nothing unless committed"]
pub struct UpdateBatch {
    origin: Publisher,
    updates: Pooled<Vec<BatchMsg>>,
    unsubscribes: Option<Pooled<Vec<(ClId, Id)>>>,
}

impl UpdateBatch {
    /// return the number of queued updates in the batch
    pub fn len(&self) -> usize {
        self.updates.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &BatchMsg> {
        self.updates.iter()
    }

    /// Commit this batch, triggering all queued values to be
    /// sent. Any subscriber that can't accept all the updates within
    /// `timeout` will be disconnected.
    pub async fn commit(mut self, timeout: Option<Duration>) {
        let empty = self.updates.is_empty()
            && self.unsubscribes.as_ref().map(|v| v.len()).unwrap_or(0) == 0;
        if empty {
            return;
        }
        let fut = {
            let mut batch = BATCH.take();
            let mut pb = self.origin.0.lock();
            for m in self.updates.drain(..) {
                match m {
                    BatchMsg::Update(None, id, v) => {
                        if let Some(pbl) = pb.by_id.get_mut(&id) {
                            for cl in pbl.subscribed.iter() {
                                batch
                                    .entry(*cl)
                                    .or_insert_with(Update::new)
                                    .updates
                                    .push(ToSubscriber::Update(id, v.clone()));
                            }
                            pbl.current = v;
                        }
                    }
                    BatchMsg::Update(Some(cl), id, v) => batch
                        .entry(cl)
                        .or_insert_with(Update::new)
                        .updates
                        .push(ToSubscriber::Update(id, v)),
                }
            }
            if let Some(usubs) = &mut self.unsubscribes {
                for (cl, id) in usubs.drain(..) {
                    let update = batch.entry(cl).or_insert_with(Update::new);
                    match &mut update.unsubscribes {
                        Some(u) => u.push(id),
                        None => {
                            let mut u = UNSUBS.take();
                            u.push(id);
                            update.unsubscribes = Some(u);
                        }
                    }
                }
            }
            future::join_all(
                batch
                    .drain()
                    .filter_map(|(cl, batch)| {
                        pb.clients.get(&cl).map(move |cl| (cl.msg_queue.clone(), batch))
                    })
                    .map(|(mut q, batch)| async move {
                        let _: Result<_, _> = q.send((timeout, batch)).await;
                    }),
            )
        };
        fut.await;
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

    pub async fn new(bind: SocketAddr, slack: usize) -> Result<Self> {
        let listener = TcpListener::bind(bind).await?;
        let (stop, receive_stop) = oneshot::channel();
        let (tx_trigger, rx_trigger) = unbounded();
        let pb = Publisher(Arc::new(Mutex::new(PublisherInner {
            addr: bind,
            stop: Some(stop),
            clients: HashMap::default(),
            hc_subscribed: HashMap::default(),
            by_path: HashMap::new(),
            by_id: HashMap::default(),
            advertised: HashMap::new(),
            to_publish: TOPUB.take(),
            to_unpublish: TOUPUB.take(),
            to_unsubscribe: TOUSUB.take(),
            publish_triggered: false,
            trigger_publish: tx_trigger,
        })));
        task::spawn({
            let pb_weak = pb.downgrade();
            async move {
                run(pb_weak.clone(), listener, receive_stop, slack).await;
                info!("accept loop shutdown");
            }
        });
        Ok(pb)
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
    pub fn publish<T>(&self, path: Path, init: T) -> Result<Val>
    where
        T: Pack,
    {
        let init: Bytes = utils::pack(&init)?.freeze();
        let id = Id::new();
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
        pb.by_path.insert(path.clone(), id);
        pb.to_unpublish.remove(&path);
        pb.to_publish.insert(path.clone());
        pb.trigger_publish();
        Ok(Val(id))
    }

    /// Start a new update batch. Updates are queued in the batch (see
    /// `Val::update`), and then the batch can be either discarded, or
    /// committed. If discarded then none of the updates will have any
    /// effect, otherwise once committed the queued updates will be
    /// sent out to subscribers and also will effect the current value
    /// given to new subscribers.
    ///
    /// Multiple batches may be started concurrently.
    pub fn start_batch(&self) -> UpdateBatch {
        UpdateBatch { origin: self.clone(), updates: RAWBATCH.take(), unsubscribes: None }
    }

    /// Wait until all previous publish or unpublish commands have
    /// been processed by the resolver server. e.g. if you just
    /// published 100 values, and you want to know when they have been
    /// uploaded to the resolver, you can call `flushed`.
    pub async fn flushed(&self) {
        let (tx, rx) = oneshot::channel();
        let _: Result<_, _> = self.0.lock().trigger_publish.unbounded_send(Some(tx));
        let _ = rx.await;
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
    to_unsubscribe: Pooled<HashMap<Id, Subscribed>>,
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

    fn destroy_val(&mut self, id: Id) {
        if let Some(pbl) = self.by_id.remove(&id) {
            let path = pbl.path;
            for path in iter::once(&path).chain(pbl.aliases.iter().flat_map(|v| v.iter()))
            {
                self.unpublish(path)
            }
            // self.send_event(Event::Destroyed(id));
            if pbl.subscribed.len() > 0 {
                self.to_unsubscribe.insert(id, pbl.subscribed);
            }
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

struct ClientCtx {
    publisher: PublisherWeak,
    client: ClId,
    batch: Vec<ToPublisher>,
    flushing_updates: bool,
    flush_timeout: Option<Duration>,
}

impl ClientCtx {
    fn new(client: ClId, publisher: PublisherWeak) -> Self {
        Self {
            publisher,
            client,
            batch: Vec::new(),
            flushing_updates: false,
            flush_timeout: None,
        }
    }

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

    fn handle_updates(
        &mut self,
        con: &mut WriteChannel,
        (timeout, mut up): (Option<Duration>, Update),
    ) -> Result<()> {
        for m in up.updates.drain(..) {
            con.queue_send(&m)?
        }
        if let Some(usubs) = &mut up.unsubscribes {
            for id in usubs.drain(..) {
                self.batch.push(ToPublisher::Unsubscribe(id));
            }
        }
        if self.batch.len() > 0 {
            self.handle_batch(con)?;
        }
        if con.bytes_queued() > 0 {
            self.flushing_updates = true;
            self.flush_timeout = timeout;
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
        async fn read_updates(
            flushing: bool,
            c: &mut Receiver<(Option<Duration>, Update)>,
        ) -> Option<(Option<Duration>, Update)> {
            if flushing {
                future::pending().await
            } else {
                c.next().await
            }
        }
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
                u = read_updates(self.flushing_updates, &mut updates).fuse() => {
                    match u {
                        None => break Ok(()),
                        Some(u) => self.handle_updates(&mut write_con, u)?,
                    }
                },
            }
        }
    }
}

pub async fn run(
    t: PublisherWeak,
    listener: TcpListener,
    stop: oneshot::Receiver<()>,
    slack: usize,
) {
    let mut stop = stop.fuse();
    loop {
        select_biased! {
            _ = stop => break,
            cl = listener.accept().fuse() => match cl {
                Err(e) => error!("failed to accept connection: {e:?}"),
                Ok((s, addr)) => {
                    debug!("accepted client: {addr:?}");
                    try_continue!("set nodelay", s.set_nodelay(true));
                    let clid = ClId::new();
                    let t_weak = t.clone();
                    let t = match t.upgrade() {
                        None => return,
                        Some(t) => t
                    };
                    let mut pb = t.0.lock();
                    let (tx, rx) = channel(slack);
                    pb.clients.insert(clid, PublisherClient {
                        msg_queue: tx,
                        subscribed: HashSet::default(),
                        auth: None,
                    });
                    task::spawn(async move {
                        let ctx = ClientCtx::new(clid, t_weak.clone());
                        let r = ctx.run(s, rx).await;
                        info!("accept_loop client shutdown {:?}", r);
                        if let Some(t) = t_weak.upgrade() {
                            let mut pb = t.0.lock();
                            if let Some(cl) = pb.clients.remove(&clid) {
                                for id in cl.subscribed {
                                    pb.unsubscribe(clid, id);
                                }
                                pb.hc_subscribed.retain(|_, v| {
                                    Arc::get_mut(v).is_none()
                                });
                            }
                        }
                    });
                }
            }
        }
    }
}
