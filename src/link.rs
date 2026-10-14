//! The connection to the broker. It lives on a thread of its own and never
//! blocks the plugin.
//!
//! While the broker is away nothing piles up without bound: the latest of
//! each state message (a system's state) is kept, and a bounded number of
//! per-call events; both go out when it's back. What's over is dropped and
//! counted.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rumqttc::{Client, Event, LastWill, MqttOptions, Packet, QoS, Transport};
use trunk_recorder_plugin::{Host, State};

use crate::messages::{Message, Publisher};

/// Messages the client's own queue takes (it holds what's sent while the connection is up).
const QUEUE: usize = 512;
/// Per-call events kept while the broker is away; the oldest go first.
const EVENTS_HELD: usize = 256;

pub struct Settings {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: String,
    pub password: String,
    pub client_id: String,
    pub qos: QoS,
    /// The message the broker sends if the plugin vanishes: topic and payload.
    pub will: (String, String),
}

/// What waits for the broker, and whether it's there.
#[derive(Default)]
struct Held {
    up: bool,
    /// The latest of each topic's states.
    latest: BTreeMap<String, Message>,
    events: VecDeque<Message>,
}

#[derive(Clone)]
pub struct Link {
    client: Client,
    qos: QoS,
    held: Arc<Mutex<Held>>,
    dropped: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    /// Messages queued, written to the broker, and (QoS 1) acknowledged by it.
    queued: Arc<AtomicU64>,
    sent: Arc<AtomicU64>,
    acked: Arc<AtomicU64>,
    /// Set when closing: nothing is replayed then.
    closing: Arc<AtomicBool>,
}

impl Link {
    /// Connect (and keep connecting). `on_connect` is sent at every connect, before anything else;
    /// `connected` is called once, after the first connect has sent them.
    pub fn start(s: Settings, on_connect: Vec<Message>, host: Host, connected: impl FnOnce() + Send + 'static) -> Result<Link, String> {
        let mut o = MqttOptions::new(&s.client_id, &s.host, s.port);
        o.set_keep_alive(Duration::from_secs(30));
        if !s.username.is_empty() {
            o.set_credentials(&s.username, &s.password);
        }
        o.set_last_will(LastWill::new(&s.will.0, s.will.1.clone(), QoS::AtLeastOnce, true));
        if s.tls {
            o.set_transport(Transport::tls_with_default_config());
        }
        // QoS 1 means a message survives a dropped connection: the broker has to keep the session for that.
        o.set_clean_session(s.qos == QoS::AtMostOnce);
        let (client, mut conn) = Client::new(o, QUEUE);
        let link = Link {
            client,
            qos: s.qos,
            held: Arc::default(),
            dropped: Arc::default(),
            done: Arc::default(),
            queued: Arc::default(),
            sent: Arc::default(),
            acked: Arc::default(),
            closing: Arc::default(),
        };
        let (me, where_) = (link.clone(), format!("{}:{}", s.host, s.port));
        std::thread::Builder::new()
            .name("mqtt".into())
            .spawn(move || {
                let mut up = false;
                // Told once the first connect's messages have gone (QoS 0) or been acknowledged (QoS 1): the count to reach, and what to do.
                let mut connected = Some(connected);
                let mut target: Option<u64> = None;
                // What a connect has to say, until the client's queue has taken all of it.
                let mut replay: Vec<Message> = Vec::new();
                for ev in conn.iter() {
                    match ev {
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            up = true;
                            host.info(format!("connected to {where_}"));
                            host.status(State::Ok, format!("connected to {where_}"));
                            if !me.closing.load(Ordering::SeqCst) {
                                replay = on_connect.clone();
                            }
                        }
                        Ok(Event::Outgoing(rumqttc::Outgoing::Disconnect)) => break,
                        Ok(Event::Outgoing(rumqttc::Outgoing::Publish(_))) => {
                            me.sent.fetch_add(1, Ordering::SeqCst);
                        }
                        Ok(Event::Incoming(Packet::PubAck(_))) => {
                            me.acked.fetch_add(1, Ordering::SeqCst);
                        }
                        Ok(_) => {}
                        Err(e) => {
                            if up {
                                host.warn(format!("lost the broker at {where_}: {e}"));
                            }
                            up = false;
                            me.held.lock().unwrap().up = false;
                            host.status(State::Warning, format!("can't reach {where_}: {e}"));
                            // The next poll reconnects; don't spin while the broker is away.
                            std::thread::sleep(Duration::from_secs(5));
                        }
                    }
                    if up && !me.closing.load(Ordering::SeqCst) {
                        replay.retain(|m| !me.try_publish(m));
                        if replay.is_empty() {
                            me.flush_held();
                            if connected.is_some() {
                                target.get_or_insert_with(|| me.queued.load(Ordering::SeqCst));
                            }
                        }
                    }
                    if let Some(t) = target {
                        let got = if me.qos == QoS::AtMostOnce { &me.sent } else { &me.acked };
                        if got.load(Ordering::SeqCst) >= t {
                            if let Some(f) = connected.take() {
                                f();
                            }
                            target = None;
                        }
                    }
                }
                me.done.store(true, Ordering::SeqCst);
            })
            .map_err(|e| format!("can't start the broker connection: {e}"))?;
        Ok(link)
    }

    /// Whether the queue took `m` (counted for [`Link::close`]).
    fn try_publish(&self, m: &Message) -> bool {
        let ok = self.client.try_publish(&m.topic, self.qos, m.retain, m.payload.clone()).is_ok();
        if ok {
            self.queued.fetch_add(1, Ordering::SeqCst);
        }
        ok
    }

    /// Send what waited for the broker, then let new messages go straight through. What the client's queue
    /// can't take yet stays held (and the link not up), to be tried again as the queue drains.
    fn flush_held(&self) {
        let mut h = self.held.lock().unwrap();
        if h.up {
            return;
        }
        while let Some(m) = h.latest.values().next().cloned() {
            if !self.try_publish(&m) {
                return;
            }
            h.latest.remove(&m.topic);
        }
        while let Some(m) = h.events.front() {
            if !self.try_publish(m) {
                return;
            }
            h.events.pop_front();
        }
        h.up = true;
    }

    /// Messages dropped: the broker was away and the plugin's room for them ran out, or the client's queue was full.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Say `last` (retained), disconnect cleanly, and wait up to `grace` for it to be sent. If it can't
    /// be queued in that time (the broker is away), don't disconnect cleanly: the broker then sends the
    /// will, which says the same.
    pub fn close(&self, last: Message, grace: Duration) {
        let t = Instant::now();
        // What's waiting for the broker goes first.
        while !(self.held.lock().unwrap().up && self.held_is_empty()) {
            if t.elapsed() >= grace {
                self.closing.store(true, Ordering::SeqCst);
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        self.closing.store(true, Ordering::SeqCst);
        while !self.try_publish(&last) {
            if t.elapsed() >= grace {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        // A disconnect right behind queued messages can lose them: let them go first.
        let settled = |s: &Link| {
            let (queued, got) =
                (s.queued.load(Ordering::SeqCst), if s.qos == QoS::AtMostOnce { s.sent.load(Ordering::SeqCst) } else { s.acked.load(Ordering::SeqCst) });
            got >= queued
        };
        while !settled(self) && !self.done.load(Ordering::SeqCst) && t.elapsed() < grace {
            std::thread::sleep(Duration::from_millis(20));
        }
        std::thread::sleep(Duration::from_millis(50));
        let _ = self.client.disconnect();
        while !self.done.load(Ordering::SeqCst) && t.elapsed() < grace {
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn held_is_empty(&self) -> bool {
        let h = self.held.lock().unwrap();
        h.latest.is_empty() && h.events.is_empty()
    }
}

impl Publisher for Link {
    fn publish(&self, m: Message) {
        let mut h = self.held.lock().unwrap();
        if !h.up {
            // The broker is away: keep the latest state, and a few events.
            if !m.event {
                h.latest.insert(m.topic.clone(), m);
            } else {
                if h.events.len() >= EVENTS_HELD {
                    h.events.pop_front();
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
                h.events.push_back(m);
            }
            return;
        }
        drop(h);
        if !self.try_publish(&m) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A link that never connects: the broker is away.
    fn away() -> (Link, rumqttc::Connection) {
        let (client, conn) = Client::new(MqttOptions::new("t", "127.0.0.1", 1), QUEUE);
        let link = Link {
            client,
            qos: QoS::AtMostOnce,
            held: Arc::default(),
            dropped: Arc::default(),
            done: Arc::default(),
            queued: Arc::default(),
            sent: Arc::default(),
            acked: Arc::default(),
            closing: Arc::default(),
        };
        (link, conn)
    }

    fn msg(topic: &str, payload: &str, retain: bool) -> Message {
        Message { topic: topic.into(), payload: payload.into(), retain, event: !retain }
    }

    #[test]
    fn while_the_broker_is_away_only_the_latest_state_and_a_few_events_are_kept() {
        let (l, _conn) = away();
        for i in 0..1000 {
            l.publish(msg("p/a/state", &format!("{i}"), true));
            l.publish(msg("p/b/state", &format!("{i}"), true));
            l.publish(msg("p/a/call", &format!("{i}"), false));
        }
        let h = l.held.lock().unwrap();
        assert_eq!(h.latest.len(), 2, "one state per topic");
        assert_eq!(h.latest["p/a/state"].payload, "999", "the latest");
        assert_eq!(h.events.len(), EVENTS_HELD);
        assert_eq!(h.events.back().unwrap().payload, "999");
        assert_eq!(h.events.front().unwrap().payload, format!("{}", 1000 - EVENTS_HELD), "the oldest went");
        assert_eq!(l.dropped(), (1000 - EVENTS_HELD) as u64);
    }

    #[test]
    fn unretained_state_cannot_crowd_out_call_events() {
        let (l, _conn) = away();
        for i in 0..1000 {
            l.publish(Message { topic: "p/a/state".into(), payload: format!("{i}"), retain: false, event: false });
        }
        for i in 0..10 {
            l.publish(Message { topic: "p/a/call".into(), payload: format!("{i}"), retain: false, event: true });
        }
        for i in 0..1000 {
            l.publish(Message { topic: "p/a/state".into(), payload: format!("{i}"), retain: false, event: false });
        }
        let h = l.held.lock().unwrap();
        assert_eq!(h.events.len(), 10, "every call is still waiting");
        assert_eq!(h.latest.len(), 1);
        assert_eq!(l.dropped(), 0);
    }

    #[test]
    fn what_the_queue_cannot_take_yet_stays_held() {
        let (client, _conn) = Client::new(MqttOptions::new("t", "127.0.0.1", 1), 2);
        let l = Link { client, ..away().0 };
        for i in 0..5 {
            l.publish(msg("p/a/call", &format!("{i}"), false));
        }
        l.flush_held();
        {
            let h = l.held.lock().unwrap();
            assert!(!h.up, "not up until everything has been taken");
            assert_eq!(h.events.len(), 3, "two went; the rest wait");
            assert_eq!(h.events.front().unwrap().payload, "2", "in order");
        }
        assert_eq!(l.dropped(), 0, "nothing was lost");
    }

    #[test]
    fn what_was_held_is_sent_when_the_broker_is_back_and_then_nothing_waits() {
        let (l, _conn) = away();
        l.publish(msg("p/a/state", "1", true));
        l.publish(msg("p/a/call", "x", false));
        l.flush_held();
        assert!(l.held.lock().unwrap().up && l.held_is_empty());
        assert_eq!(l.queued.load(Ordering::SeqCst), 2, "both went to the client");
        l.publish(msg("p/a/state", "2", true));
        assert_eq!(l.queued.load(Ordering::SeqCst), 3, "now straight through");
        assert!(l.held_is_empty());
    }
}
