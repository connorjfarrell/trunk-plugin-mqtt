//! The connection to the broker. It lives on a thread of its own and never
//! blocks the plugin: a message that can't be queued (the broker has been
//! away long enough to fill the queue) is dropped and counted.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rumqttc::{Client, Event, LastWill, MqttOptions, Packet, QoS, Transport};
use trunk_recorder_plugin::{Host, State};

use crate::messages::{Message, Publisher};

/// Messages that can wait for a broker that's away.
const QUEUE: usize = 512;

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

#[derive(Clone)]
pub struct Link {
    client: Client,
    qos: QoS,
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
    /// Connect (and keep connecting). `on_connect` is sent at every connect.
    pub fn start(s: Settings, on_connect: Vec<Message>, host: Host) -> Link {
        let mut o = MqttOptions::new(&s.client_id, &s.host, s.port);
        o.set_keep_alive(Duration::from_secs(30));
        if !s.username.is_empty() {
            o.set_credentials(&s.username, &s.password);
        }
        o.set_last_will(LastWill::new(&s.will.0, s.will.1.clone(), QoS::AtLeastOnce, true));
        if s.tls {
            o.set_transport(Transport::tls_with_default_config());
        }
        let (client, mut conn) = Client::new(o, QUEUE);
        let link = Link {
            client: client.clone(),
            qos: s.qos,
            dropped: Arc::default(),
            done: Arc::default(),
            queued: Arc::default(),
            sent: Arc::default(),
            acked: Arc::default(),
            closing: Arc::default(),
        };
        // First in the queue, so nothing published later can overtake them. They go again at every reconnect (a
        // broker that restarted without its retained messages, a session that lost what was queued).
        for m in &on_connect {
            link.publish(m.clone());
        }
        let (done, where_, sent, acked, closing, me) =
            (link.done.clone(), format!("{}:{}", s.host, s.port), link.sent.clone(), link.acked.clone(), link.closing.clone(), link.clone());
        let _ = std::thread::Builder::new().name("mqtt".into()).spawn(move || {
            let (mut up, mut connects) = (false, 0);
            // What a reconnect has to say again, until the queue has taken all of it.
            let mut replay: Vec<Message> = Vec::new();
            for ev in conn.iter() {
                match ev {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        up = true;
                        host.info(format!("connected to {where_}"));
                        host.status(State::Ok, format!("connected to {where_}"));
                        // The first connect has them already (queued at the start); a later one sends them again.
                        connects += 1;
                        if connects > 1 && !closing.load(Ordering::SeqCst) {
                            replay = on_connect.clone();
                        }
                    }
                    Ok(Event::Outgoing(rumqttc::Outgoing::Disconnect)) => break,
                    Ok(Event::Outgoing(rumqttc::Outgoing::Publish(_))) => {
                        sent.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(Event::Incoming(Packet::PubAck(_))) => {
                        acked.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        if up {
                            host.warn(format!("lost the broker at {where_}: {e}"));
                        }
                        up = false;
                        host.status(State::Warning, format!("can't reach {where_}: {e}"));
                        // The next poll reconnects; don't spin while the broker is away.
                        std::thread::sleep(Duration::from_secs(5));
                    }
                }
                if !closing.load(Ordering::SeqCst) {
                    replay.retain(|m| !me.try_publish(m));
                }
            }
            done.store(true, Ordering::SeqCst);
        });
        link
    }

    /// Messages dropped because the queue was full.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Whether the queue took `m` (counted for [`Link::close`]).
    fn try_publish(&self, m: &Message) -> bool {
        let ok = self.client.try_publish(&m.topic, self.qos, m.retain, m.payload.clone()).is_ok();
        if ok {
            self.queued.fetch_add(1, Ordering::SeqCst);
        }
        ok
    }

    /// Say `last` (retained), disconnect cleanly, and wait up to `grace` for it to be sent. If `last` can't
    /// be queued in that time, don't disconnect cleanly: the broker then sends the will, which says the same.
    pub fn close(&self, last: Message, grace: Duration) {
        let t = Instant::now();
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
}

impl Publisher for Link {
    fn publish(&self, m: Message) {
        if !self.try_publish(&m) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
