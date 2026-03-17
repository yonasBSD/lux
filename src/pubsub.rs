use bytes::Bytes;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};

const CHANNEL_CAPACITY: usize = 1024;

pub struct BlockedPopRequest {
    pub tx: mpsc::Sender<(String, Bytes)>,
    pub pop_left: bool,
    pub waiter_id: u64,
}

#[derive(Clone)]
pub struct Broker {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<Message>>>>,
    list_waiters: Arc<parking_lot::Mutex<HashMap<String, VecDeque<BlockedPopRequest>>>>,
    list_waiter_count: Arc<AtomicU64>,
    stream_waiters: Arc<parking_lot::Mutex<HashMap<String, Vec<mpsc::Sender<()>>>>>,
    waiter_counter: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
pub struct Message {
    pub channel: String,
    pub payload: String,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
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

    pub async fn subscribe(&self, channel: &str) -> broadcast::Receiver<Message> {
        let mut channels = self.channels.write().await;
        let tx = channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    pub async fn publish(&self, channel: &str, payload: String) -> i64 {
        let channels = self.channels.read().await;
        if let Some(tx) = channels.get(channel) {
            let msg = Message {
                channel: channel.to_string(),
                payload,
            };
            tx.send(msg).unwrap_or(0) as i64
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribe_and_publish() {
        let broker = Broker::new();
        let mut rx = broker.subscribe("test-channel").await;
        let count = broker.publish("test-channel", "hello".to_string()).await;
        assert_eq!(count, 1);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "test-channel");
        assert_eq!(msg.payload, "hello");
    }

    #[tokio::test]
    async fn publish_to_empty_channel_returns_zero() {
        let broker = Broker::new();
        let count = broker
            .publish("nobody-listening", "hello".to_string())
            .await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let broker = Broker::new();
        let mut rx1 = broker.subscribe("ch").await;
        let mut rx2 = broker.subscribe("ch").await;
        broker.publish("ch", "msg".to_string()).await;
        assert_eq!(rx1.recv().await.unwrap().payload, "msg");
        assert_eq!(rx2.recv().await.unwrap().payload, "msg");
    }

    #[tokio::test]
    async fn separate_channels_are_isolated() {
        let broker = Broker::new();
        let mut rx_a = broker.subscribe("a").await;
        let _rx_b = broker.subscribe("b").await;
        broker.publish("a", "only-a".to_string()).await;
        assert_eq!(rx_a.recv().await.unwrap().payload, "only-a");
    }

    #[tokio::test]
    async fn multiple_messages_in_order() {
        let broker = Broker::new();
        let mut rx = broker.subscribe("ch").await;
        broker.publish("ch", "first".to_string()).await;
        broker.publish("ch", "second".to_string()).await;
        broker.publish("ch", "third".to_string()).await;
        assert_eq!(rx.recv().await.unwrap().payload, "first");
        assert_eq!(rx.recv().await.unwrap().payload, "second");
        assert_eq!(rx.recv().await.unwrap().payload, "third");
    }
}
