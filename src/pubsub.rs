use bytes::Bytes;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

type KeySubMap = Vec<(String, broadcast::Sender<Message>)>;
type KeyEvent = (Box<[u8]>, Box<[u8]>);

const CHANNEL_CAPACITY: usize = 1024;
const KEY_EVENT_QUEUE_CAPACITY: usize = 65536;

pub struct BlockedPopRequest {
    pub tx: mpsc::Sender<(String, Bytes)>,
    pub pop_left: bool,
    pub waiter_id: u64,
}

#[derive(Clone)]
pub struct Broker {
    channels: Arc<parking_lot::RwLock<HashMap<String, broadcast::Sender<Message>>>>,
    pattern_subs: Arc<parking_lot::RwLock<HashMap<String, broadcast::Sender<Message>>>>,
    key_subs: Arc<parking_lot::RwLock<Arc<KeySubMap>>>,
    key_sub_count: Arc<AtomicU64>,
    key_event_tx: mpsc::Sender<KeyEvent>,
    key_event_rx: Arc<parking_lot::Mutex<Option<mpsc::Receiver<KeyEvent>>>>,
    list_waiters: Arc<parking_lot::Mutex<HashMap<String, VecDeque<BlockedPopRequest>>>>,
    list_waiter_count: Arc<AtomicU64>,
    stream_waiters: Arc<parking_lot::Mutex<HashMap<String, Vec<mpsc::Sender<()>>>>>,
    waiter_counter: Arc<AtomicU64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MessageKind {
    PubSub,
    KeyEvent,
}

#[derive(Clone, Debug)]
pub struct Message {
    pub channel: String,
    pub payload: String,
    pub pattern: Option<String>,
    pub kind: MessageKind,
}

impl Broker {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(KEY_EVENT_QUEUE_CAPACITY);
        Self {
            channels: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            pattern_subs: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            key_subs: Arc::new(parking_lot::RwLock::new(Arc::new(Vec::new()))),
            key_sub_count: Arc::new(AtomicU64::new(0)),
            key_event_tx: tx,
            key_event_rx: Arc::new(parking_lot::Mutex::new(Some(rx))),
            list_waiters: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            list_waiter_count: Arc::new(AtomicU64::new(0)),
            stream_waiters: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            waiter_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn next_waiter_id(&self) -> u64 {
        self.waiter_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub fn has_list_waiters(&self, _key: &str) -> bool {
        self.list_waiter_count.load(Ordering::Relaxed) > 0
    }

    pub fn register_list_waiter(&self, key: &str, req: BlockedPopRequest) {
        let mut waiters = self.list_waiters.lock();
        waiters.entry(key.to_string()).or_default().push_back(req);
        self.list_waiter_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn drain_list_waiters(
        &self,
        key: &str,
        shard_data: &mut hashbrown::HashMap<
            String,
            crate::store::Entry,
            crate::store::FxBuildHasher,
        >,
        now: std::time::Instant,
    ) {
        let mut waiters = self.list_waiters.lock();
        let queue = match waiters.get_mut(key) {
            Some(q) => q,
            None => return,
        };

        while !queue.is_empty() {
            let entry = match shard_data.get_mut(key) {
                Some(e) if !e.is_expired_at(now) => e,
                _ => return,
            };
            let list = match &mut entry.value {
                crate::store::StoreValue::List(l) if !l.is_empty() => l,
                _ => return,
            };

            let req = queue.pop_front().unwrap();
            self.list_waiter_count.fetch_sub(1, Ordering::Relaxed);
            let val = if req.pop_left {
                list.pop_front()
            } else {
                list.pop_back()
            };
            if let Some(v) = val {
                let _ = req.tx.try_send((key.to_string(), v));
            }
        }

        if queue.is_empty() {
            waiters.remove(key);
        }
    }

    pub fn remove_list_waiters_by_id(&self, keys: &[String], id: u64) {
        let mut waiters = self.list_waiters.lock();
        for key in keys {
            if let Some(queue) = waiters.get_mut(key) {
                let before = queue.len();
                queue.retain(|r| r.waiter_id != id);
                let removed = before - queue.len();
                if removed > 0 {
                    self.list_waiter_count
                        .fetch_sub(removed as u64, Ordering::Relaxed);
                }
                if queue.is_empty() {
                    waiters.remove(key);
                }
            }
        }
    }

    pub fn register_stream_waiter(&self, key: &str, tx: mpsc::Sender<()>) {
        let mut waiters = self.stream_waiters.lock();
        waiters.entry(key.to_string()).or_default().push(tx);
    }

    pub fn wake_stream_waiters(&self, key: &str) {
        let mut waiters = self.stream_waiters.lock();
        if let Some(senders) = waiters.remove(key) {
            for tx in senders {
                let _ = tx.try_send(());
            }
        }
    }

    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<Message> {
        let mut channels = self.channels.write();
        let tx = channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    pub fn psubscribe(&self, pattern: &str) -> broadcast::Receiver<Message> {
        let mut patterns = self.pattern_subs.write();
        let tx = patterns
            .entry(pattern.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    pub fn publish(&self, channel: &str, payload: String) -> i64 {
        let mut count = 0i64;
        {
            let channels = self.channels.read();
            if let Some(tx) = channels.get(channel) {
                let msg = Message {
                    channel: channel.to_string(),
                    payload: payload.clone(),
                    pattern: None,
                    kind: MessageKind::PubSub,
                };
                count += tx.send(msg).unwrap_or(0) as i64;
            }
        }
        {
            let patterns = self.pattern_subs.read();
            for (pat, tx) in patterns.iter() {
                if glob_match(pat, channel) {
                    let msg = Message {
                        channel: channel.to_string(),
                        payload: payload.clone(),
                        pattern: Some(pat.clone()),
                        kind: MessageKind::PubSub,
                    };
                    count += tx.send(msg).unwrap_or(0) as i64;
                }
            }
        }
        count
    }

    pub fn ksubscribe(&self, pattern: &str) -> broadcast::Receiver<Message> {
        let mut subs = self.key_subs.write();
        let inner = Arc::make_mut(&mut subs);
        if let Some((_, tx)) = inner.iter().find(|(p, _)| p == pattern) {
            return tx.subscribe();
        }
        let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
        inner.push((pattern.to_string(), tx));
        self.key_sub_count
            .store(inner.len() as u64, Ordering::Release);
        rx
    }

    pub fn kunsub(&self, pattern: &str) {
        let mut subs = self.key_subs.write();
        let inner = Arc::make_mut(&mut subs);
        inner.retain(|(p, _)| p != pattern);
        self.key_sub_count
            .store(inner.len() as u64, Ordering::Release);
    }

    #[inline(always)]
    pub fn has_key_subs(&self) -> bool {
        self.key_sub_count.load(Ordering::Relaxed) > 0
    }

    #[inline(always)]
    pub fn enqueue_key_event(&self, key: &[u8], cmd: &[u8]) {
        if self.key_sub_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let _ = self.key_event_tx.try_send((key.into(), cmd.into()));
    }

    pub fn take_key_event_rx(&self) -> Option<mpsc::Receiver<KeyEvent>> {
        self.key_event_rx.lock().take()
    }

    fn emit_key_event(subs: &[(String, broadcast::Sender<Message>)], key: &str, cmd: &[u8]) {
        let mut op: Option<String> = None;
        for (pat, tx) in subs.iter() {
            if glob_match(pat, key) {
                let operation = op.get_or_insert_with(|| {
                    std::str::from_utf8(cmd).unwrap_or("").to_ascii_lowercase()
                });
                let msg = Message {
                    channel: key.to_string(),
                    payload: operation.clone(),
                    pattern: Some(pat.clone()),
                    kind: MessageKind::KeyEvent,
                };
                let _ = tx.send(msg);
            }
        }
    }

    pub async fn run_key_event_loop(self) {
        let mut rx = match self.take_key_event_rx() {
            Some(rx) => rx,
            None => return,
        };
        while let Some((key, cmd)) = rx.recv().await {
            let snap = self.key_subs.read().clone();
            if snap.is_empty() {
                continue;
            }
            if let Ok(key_str) = std::str::from_utf8(&key) {
                Self::emit_key_event(&snap, key_str, &cmd);
            }
        }
    }
}

fn glob_match(pattern: &str, s: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        if !prefix.contains(['?', '[', '*']) {
            return s.starts_with(prefix);
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let sc: Vec<char> = s.chars().collect();
    do_glob(&p, &sc, 0, 0)
}

fn do_glob(p: &[char], s: &[char], pi: usize, si: usize) -> bool {
    if pi == p.len() && si == s.len() {
        return true;
    }
    if pi == p.len() {
        return false;
    }
    match p[pi] {
        '*' => {
            for i in si..=s.len() {
                if do_glob(p, s, pi + 1, i) {
                    return true;
                }
            }
            false
        }
        '?' if si < s.len() => do_glob(p, s, pi + 1, si + 1),
        '[' if si < s.len() => {
            let mut j = pi + 1;
            let negate = j < p.len() && p[j] == '^';
            if negate {
                j += 1;
            }
            let mut matched = false;
            while j < p.len() && p[j] != ']' {
                if j + 2 < p.len() && p[j + 1] == '-' {
                    if s[si] >= p[j] && s[si] <= p[j + 2] {
                        matched = true;
                    }
                    j += 3;
                } else {
                    if s[si] == p[j] {
                        matched = true;
                    }
                    j += 1;
                }
            }
            if negate {
                matched = !matched;
            }
            if matched && j < p.len() {
                do_glob(p, s, j + 1, si + 1)
            } else {
                false
            }
        }
        c if si < s.len() && c == s[si] => do_glob(p, s, pi + 1, si + 1),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_and_publish() {
        let broker = Broker::new();
        let mut rx = broker.subscribe("test-channel");
        let count = broker.publish("test-channel", "hello".to_string());
        assert_eq!(count, 1);
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.channel, "test-channel");
        assert_eq!(msg.payload, "hello");
    }

    #[test]
    fn publish_to_empty_channel_returns_zero() {
        let broker = Broker::new();
        let count = broker.publish("nobody-listening", "hello".to_string());
        assert_eq!(count, 0);
    }

    #[test]
    fn multiple_subscribers() {
        let broker = Broker::new();
        let mut rx1 = broker.subscribe("ch");
        let mut rx2 = broker.subscribe("ch");
        broker.publish("ch", "msg".to_string());
        assert_eq!(rx1.try_recv().unwrap().payload, "msg");
        assert_eq!(rx2.try_recv().unwrap().payload, "msg");
    }

    #[test]
    fn separate_channels_are_isolated() {
        let broker = Broker::new();
        let mut rx_a = broker.subscribe("a");
        let _rx_b = broker.subscribe("b");
        broker.publish("a", "only-a".to_string());
        assert_eq!(rx_a.try_recv().unwrap().payload, "only-a");
    }

    #[test]
    fn multiple_messages_in_order() {
        let broker = Broker::new();
        let mut rx = broker.subscribe("ch");
        broker.publish("ch", "first".to_string());
        broker.publish("ch", "second".to_string());
        broker.publish("ch", "third".to_string());
        assert_eq!(rx.try_recv().unwrap().payload, "first");
        assert_eq!(rx.try_recv().unwrap().payload, "second");
        assert_eq!(rx.try_recv().unwrap().payload, "third");
    }
}
