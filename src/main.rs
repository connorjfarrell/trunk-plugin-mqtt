//! mqtt — a Trunk Recorder Pro plugin that publishes each system's state and
//! every recorded call to an MQTT broker: for Home Assistant (with its
//! discovery), and for Telegraf and Grafana (Telegraf's `mqtt_consumer`).
//!
//! It only watches: it keeps no files, so every concluded call is reported as
//! skipped and nothing waits on it.

mod link;
mod messages;
mod report;

use std::time::Duration;

use rumqttc::QoS;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trunk_recorder_plugin::{CallInfo, ConcludedCall, Host, Manifest, Outcome, Plugin, Setup, Status, topic};

use crate::link::Link;
use crate::messages::Message;
use crate::report::{Options, Reporter, SystemLabel};

/// The plugin's settings.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
struct Config {
    /// Broker host
    ///
    /// The MQTT broker's name or address (Mosquitto, or Home Assistant's).
    host: String,
    /// Port
    ///
    /// 1883 is plain MQTT; 8883 is the usual port for TLS.
    #[schemars(range(min = 1, max = 65535))]
    port: u16,
    /// Use TLS
    tls: bool,
    /// Username
    ///
    /// Leave empty for a broker that needs none.
    username: String,
    /// Password
    #[schemars(extend("x-secret" = true))]
    password: String,
    /// Client ID
    ///
    /// Names this recorder to the broker, and in Home Assistant's device ids. Use a different one for each recorder.
    client_id: String,
    /// Topic prefix
    ///
    /// Everything is published under it: `<prefix>/status`, `<prefix>/<system>/state` and `<prefix>/<system>/call`.
    topic_prefix: String,
    /// Keep the latest state on the broker
    ///
    /// Retained: a program that connects later sees each system's last state at once. Per-call events are never retained.
    retain: bool,
    /// Quality of service
    ///
    /// 0: at most once. 1: at least once (a little more traffic).
    #[schemars(range(min = 0, max = 1))]
    qos: u8,
    /// Publish every call
    ///
    /// One message per recorded call, on `<prefix>/<system>/call`, for Telegraf and Grafana. Turn off to publish state only.
    publish_calls: bool,
    /// Home Assistant discovery
    ///
    /// Make each system a device in Home Assistant, with sensors for its decode rate, calls and control channel.
    home_assistant: bool,
    /// Discovery prefix
    ///
    /// Home Assistant's MQTT discovery prefix (`homeassistant` unless you changed it there).
    discovery_prefix: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "localhost".into(),
            port: 1883,
            tls: false,
            username: String::new(),
            password: String::new(),
            client_id: "trunk-pro".into(),
            topic_prefix: "trunk-pro".into(),
            retain: true,
            qos: 0,
            publish_calls: true,
            home_assistant: false,
            discovery_prefix: "homeassistant".into(),
        }
    }
}

/// Settings for each system.
#[derive(Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase", default)]
struct SystemConfig {
    /// Leave this system out
    skip: bool,
    /// Name
    ///
    /// What the system is called in topics and Home Assistant, instead of its short name.
    label: String,
}

struct Mqtt {
    reporter: Reporter,
    link: Link,
    prefix: String,
    host: Host,
    reported_drops: u64,
}

/// What's wrong with the settings, if anything.
fn check(c: &Config) -> Result<(), String> {
    if c.host.trim().is_empty() {
        return Err("Broker host is empty".into());
    }
    if c.port == 0 {
        return Err("Port must be 1 to 65535".into());
    }
    if c.qos > 1 {
        return Err(format!("Quality of service must be 0 or 1, not {}", c.qos));
    }
    if c.client_id.trim().is_empty() {
        return Err("Client ID is empty".into());
    }
    let p = &c.topic_prefix;
    if p.trim().is_empty() || p.starts_with('/') || p.ends_with('/') || p.contains(['+', '#', '\0']) {
        return Err("Topic prefix must be a topic like trunk-pro, without wildcards or a slash at either end".into());
    }
    if c.home_assistant && (c.discovery_prefix.trim().is_empty() || c.discovery_prefix.contains(['+', '#', '\0'])) {
        return Err("Discovery prefix must be a topic like homeassistant".into());
    }
    Ok(())
}

/// Systems that would share a topic (`County Fire` and `County/Fire` are both `County_Fire`) overwrite each other.
fn unique_levels(systems: &[SystemLabel]) -> Result<(), String> {
    let mut seen: Vec<(String, &str)> = Vec::new();
    for s in systems.iter().filter(|s| !s.skip) {
        let l = messages::level(&s.label);
        if let Some((_, other)) = seen.iter().find(|(x, _)| *x == l) {
            return Err(format!("Systems {other} and {} would both publish to topic level {l}: give one a different Name", s.short_name));
        }
        seen.push((l, &s.short_name));
    }
    Ok(())
}

impl Plugin for Mqtt {
    type Config = Config;
    type SystemConfig = SystemConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "MQTT statistics".into(),
            // Not `unit` or `audio`: they're busy, and nothing here uses them.
            subscribe: vec![topic::STATUS.into(), topic::CALL_START.into(), topic::CALL_CONCLUDED.into()],
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, SystemConfig>) -> Result<Self, String> {
        let c = &setup.config;
        check(c)?;
        let systems: Vec<SystemLabel> = setup
            .systems
            .iter()
            .map(|s| {
                let sc = s.config.as_ref();
                let label = sc.map(|c| c.label.trim()).filter(|l| !l.is_empty()).unwrap_or(&s.short_name).to_string();
                SystemLabel { short_name: s.short_name.clone(), label, skip: sc.is_some_and(|c| c.skip) }
            })
            .collect();
        unique_levels(&systems)?;
        let opt = Options {
            prefix: c.topic_prefix.clone(),
            retain: c.retain,
            publish_calls: c.publish_calls,
            discovery: c.home_assistant.then(|| c.discovery_prefix.clone()),
            node: c.client_id.clone(),
        };
        // (`check` has seen that it's 0 or 1.)
        let qos = if c.qos == 0 { QoS::AtMostOnce } else { QoS::AtLeastOnce };
        let will = (messages::status_topic(&c.topic_prefix), "offline".to_string());
        let settings = link::Settings {
            host: c.host.clone(),
            port: c.port,
            tls: c.tls,
            username: c.username.clone(),
            password: c.password.clone(),
            client_id: c.client_id.clone(),
            qos,
            will,
        };
        // The link and the reporter are made together: the link replays what the reporter says at every connect.
        let mut first = Reporter::new(opt.clone(), systems.clone(), Box::new(NoPublish));
        // The broker keeps what was published retained until it's emptied: clear what an earlier run published that this one doesn't.
        let seen = setup.data_dir.join(RETAINED_FILE);
        first.clean_up(&read_topics(&seen));
        let now = first.retained_topics();
        let (note, write_host) = (seen.clone(), host.clone());
        let on_connect = first.on_connect();
        // The set is remembered once the broker has been told it (if it never is, the next run clears what this one would have).
        let link = Link::start(settings, on_connect, host.clone(), move || write_topics(&note, &now, &write_host));
        let prefix = c.topic_prefix.clone();
        host.info(format!("publishing under {prefix}/ to {}:{}", c.host, c.port));
        Ok(Mqtt { reporter: Reporter::new(opt, systems, Box::new(link.clone())), link, prefix, host, reported_drops: 0 })
    }

    fn status(&mut self, status: Status) {
        self.reporter.status(&status);
        let d = self.link.dropped();
        if d > self.reported_drops {
            self.host.warn(format!("{d} message(s) dropped so far: the broker has been away too long, or can't keep up"));
            self.reported_drops = d;
        }
    }

    fn call_start(&mut self, call: CallInfo) {
        self.reporter.call_start(&call);
    }

    fn call_concluded(&mut self, call: ConcludedCall) {
        self.reporter.call_concluded(&call);
        // It keeps nothing of the call, so nothing waits on it.
        self.host.call_result(&call.path, Outcome::Skipped, "statistics only", "");
    }

    fn shutdown(&mut self, grace: Duration) {
        let last = Message { topic: messages::status_topic(&self.prefix), payload: "offline".into(), retain: true };
        self.link.close(last, grace.min(Duration::from_secs(3)));
    }
}

/// The retained topics the last run published, kept in the plugin's data folder.
const RETAINED_FILE: &str = "retained-topics.json";

fn read_topics(path: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(path).ok().and_then(|t| serde_json::from_str(&t).ok()).unwrap_or_default()
}

fn write_topics(path: &std::path::Path, topics: &[String], host: &Host) {
    if let Err(e) = std::fs::write(path, serde_json::to_string(topics).unwrap_or_default()) {
        host.warn(format!("can't write {}: {e}", path.display()));
    }
}

/// For making the connect messages before the link exists.
struct NoPublish;

impl messages::Publisher for NoPublish {
    fn publish(&self, _: Message) {}
}

fn main() {
    trunk_recorder_plugin::run::<Mqtt>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_are_checked() {
        assert!(check(&Config::default()).is_ok());
        for bad in [
            Config { host: " ".into(), ..Default::default() },
            Config { client_id: "".into(), ..Default::default() },
            Config { topic_prefix: "".into(), ..Default::default() },
            Config { topic_prefix: "a/#".into(), ..Default::default() },
            Config { topic_prefix: "/a".into(), ..Default::default() },
            Config { topic_prefix: "a/".into(), ..Default::default() },
            Config { home_assistant: true, discovery_prefix: "".into(), ..Default::default() },
        ] {
            assert!(check(&bad).is_err());
        }
        assert!(check(&Config { topic_prefix: "radio/trunk".into(), ..Default::default() }).is_ok(), "a prefix may have levels");
    }

    #[test]
    fn retained_topics_are_remembered_between_runs() {
        let dir = trunk_recorder_plugin::testing::temp_dir("mqtt-topics");
        let f = dir.join(RETAINED_FILE);
        assert!(read_topics(&f).is_empty(), "none yet");
        let host = trunk_recorder_plugin::testing::capture().0;
        write_topics(&f, &["a/b/config".to_string()], &host);
        assert_eq!(read_topics(&f), ["a/b/config"]);
        std::fs::write(&f, "not json").unwrap();
        assert!(read_topics(&f).is_empty(), "a damaged file is no memory");
    }

    #[test]
    fn qos_and_port_must_be_valid() {
        assert!(check(&Config { qos: 1, ..Default::default() }).is_ok());
        assert!(check(&Config { qos: 2, ..Default::default() }).is_err());
        assert!(check(&Config { port: 0, ..Default::default() }).is_err());
    }

    #[test]
    fn systems_cannot_share_a_topic() {
        let sys = |n: &str, l: &str, skip| SystemLabel { short_name: n.into(), label: l.into(), skip };
        assert!(unique_levels(&[sys("a", "County Fire", false), sys("b", "City", false)]).is_ok());
        let e = unique_levels(&[sys("a", "County Fire", false), sys("b", "County/Fire", false)]).unwrap_err();
        assert!(e.contains("a and b"), "{e}");
        assert!(unique_levels(&[sys("a", "X", false), sys("b", "X", true)]).is_ok(), "a system left out takes no topic");
    }

    #[test]
    fn the_manifest_subscribes_to_what_it_uses() {
        let m = Mqtt::manifest();
        assert_eq!(m.subscribe, ["status", "call.start", "call.concluded"]);
    }

    /// Against a real broker: `MQTT_TEST_ADDR=127.0.0.1:1883 cargo test -- --ignored`.
    #[test]
    #[ignore = "needs a broker: set MQTT_TEST_ADDR"]
    fn publishes_to_a_real_broker() {
        use rumqttc::{Client, Event, MqttOptions, Packet};
        use serde_json::{Value, json};
        use trunk_recorder_plugin::{HostMessage, SystemStatus, testing};

        let addr = std::env::var("MQTT_TEST_ADDR").expect("MQTT_TEST_ADDR");
        let (host, port) = addr.split_once(':').expect("host:port");
        let mut o = MqttOptions::new("e2e-listener", host, port.parse().unwrap());
        o.set_keep_alive(Duration::from_secs(5));
        let (client, mut conn) = Client::new(o, 64);
        client.subscribe("e2e/#", QoS::AtLeastOnce).unwrap();
        client.subscribe("homeassistant/#", QoS::AtLeastOnce).unwrap();
        let (tx, rx) = std::sync::mpsc::channel::<(String, String, bool)>();
        std::thread::spawn(move || {
            for ev in conn.iter() {
                if let Ok(Event::Incoming(Packet::Publish(p))) = ev {
                    let _ = tx.send((p.topic.clone(), String::from_utf8_lossy(&p.payload).into_owned(), p.retain));
                }
            }
        });
        std::thread::sleep(Duration::from_millis(500));

        let dir = testing::temp_dir("mqtt");
        let mut hello = testing::hello(
            &dir,
            json!({ "host": host, "port": port.parse::<u16>().unwrap(), "topicPrefix": "e2e", "clientId": "e2e-recorder", "homeAssistant": true, "qos": 1 }),
        );
        hello.systems[0].config = json!({ "label": "County" });
        let call = testing::call(&dir, "sys1", 101);
        let status = Status {
            time: 10.0,
            systems: vec![SystemStatus {
                short_name: "sys1".into(),
                control_channel_hz: Some(851_012_500),
                decode_rate: 38.5,
                active_calls: 1,
                recording: 1,
                ..Default::default()
            }],
        };
        let out = testing::run::<Mqtt>([HostMessage::Hello(hello), HostMessage::CallConcluded(call.clone()), HostMessage::Status(status)]);
        assert!(out.ready());
        assert_eq!(out.results(), vec![(call.path, Outcome::Skipped, "statistics only".to_string(), String::new())]);

        let mut seen: Vec<(String, String, bool)> = Vec::new();
        let until = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < until {
            if let Ok(m) = rx.recv_timeout(Duration::from_millis(200)) {
                seen.push(m);
            }
        }
        let find = |t: &str| seen.iter().filter(|m| m.0 == t).collect::<Vec<_>>();
        assert!(find("e2e/status").iter().any(|m| m.1 == "online"), "online: {seen:?}");
        assert_eq!(find("e2e/status").last().unwrap().1, "offline", "it says goodbye");
        let state: Value = serde_json::from_str(&find("e2e/County/state").last().expect("state").1).unwrap();
        assert_eq!((state["decode_rate"].clone(), state["recorded_total"].clone()), (json!(38.5), json!(1)));
        let call: Value =
            serde_json::from_str(&find("e2e/County/call").first().unwrap_or_else(|| panic!("call: {:?}", seen.iter().map(|m| &m.0).collect::<Vec<_>>())).1)
                .unwrap();
        assert_eq!((call["talkgroup"].clone(), call["system"].clone()), (json!(101), json!("County")));
        assert!(seen.iter().any(|m| m.0.starts_with("homeassistant/sensor/e2e-recorder_County/decode_rate")), "discovery");
    }
}
