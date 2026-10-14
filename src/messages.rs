//! What gets published: topics and JSON payloads, built from the recorder's
//! events. Nothing here touches the network, so it's all unit tested.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use trunk_recorder_plugin::{CallInfo, ConcludedCall, SystemStatus};

/// One message for the broker.
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    pub topic: String,
    pub payload: String,
    pub retain: bool,
    /// One of a stream (a call), none of which may stand in for another; the rest are states, of which only the latest matters.
    pub event: bool,
}

/// Where messages go: the broker link, or a recording of them in tests.
pub trait Publisher: Send {
    fn publish(&self, m: Message);
}

/// What one system has done since the plugin started.
#[derive(Clone, Debug, Default)]
pub struct Counts {
    pub calls: u64,
    pub recorded: u64,
    pub encrypted: u64,
    /// Calls not recorded, by the recorder's reason.
    pub not_recorded: BTreeMap<String, u64>,
    pub audio_seconds: f64,
    pub last_call: Option<Value>,
}

impl Counts {
    pub fn started(&mut self, c: &CallInfo) {
        self.calls += 1;
        if c.encrypted {
            self.encrypted += 1;
        }
        if !c.recording {
            *self.not_recorded.entry(c.reason.clone().unwrap_or_else(|| "other".into())).or_default() += 1;
        }
    }

    pub fn concluded(&mut self, c: &ConcludedCall) {
        self.recorded += 1;
        self.audio_seconds += c.call.call_length;
        self.last_call = Some(json!({
            "time": c.call.start_time,
            "talkgroup": c.call.talkgroup,
            "tag": c.call.talkgroup_tag,
            "length_s": c.call.call_length,
        }));
    }
}

/// A topic level from a name: no `/`, wildcards or spaces.
pub fn level(name: &str) -> String {
    let s: String = name.chars().map(|c| if matches!(c, '/' | '+' | '#' | '\0') || c.is_whitespace() { '_' } else { c }).collect();
    if s.is_empty() { "_".into() } else { s }
}

pub fn status_topic(prefix: &str) -> String {
    format!("{prefix}/status")
}

pub fn state_topic(prefix: &str, system: &str) -> String {
    format!("{prefix}/{}/state", level(system))
}

pub fn call_topic(prefix: &str, system: &str) -> String {
    format!("{prefix}/{}/call", level(system))
}

/// The system's state, as of a status tick.
pub fn state(time: f64, s: &SystemStatus, n: &Counts) -> Value {
    json!({
        "time": time,
        "control_channel_hz": s.control_channel_hz,
        "control_channel_mhz": s.control_channel_hz.map(|hz| hz as f64 / 1e6),
        "locked": u8::from(s.control_channel_hz.is_some()),
        "decode_rate": s.decode_rate,
        "active_calls": s.active_calls,
        "recording": s.recording,
        "calls_total": n.calls,
        "recorded_total": n.recorded,
        "encrypted_total": n.encrypted,
        "not_recorded_total": n.not_recorded.values().sum::<u64>(),
        "not_recorded": n.not_recorded,
        "audio_seconds_total": (n.audio_seconds * 10.0).round() / 10.0,
        "last_call": n.last_call,
    })
}

/// A recorded call, for the per-call stream (what Telegraf turns into points).
pub fn call(system: &str, c: &ConcludedCall) -> Value {
    let r = &c.call;
    let units: BTreeSet<i64> = r.src_list.iter().map(|s| s.src).collect();
    let mut v = json!({
        "time": r.start_time,
        "system": system,
        "talkgroup": r.talkgroup,
        "tag": r.talkgroup_tag,
        "group": r.talkgroup_group,
        "freq_hz": r.freq,
        "length_s": r.call_length,
        "audio_type": r.audio_type,
        "tdma_slot": r.tdma_slot,
        "emergency": u8::from(r.emergency),
        "encrypted": u8::from(r.encrypted),
        "units": units,
        "unit_count": units.len(),
        "error_count": r.error_count(),
        "spike_count": r.spike_count(),
    });
    // What the recorder measured, when it did (null when it didn't).
    for k in ["snr", "signal", "noise", "clean_voice_pct", "freq_error"] {
        if let Some(x) = r.extra.get(k).filter(|x| x.is_number()) {
            v[k] = x.clone();
        }
    }
    if let Some(d) = r.extra.get("decrypted").and_then(Value::as_i64) {
        v["decrypted"] = json!(u8::from(d != 0));
    }
    v
}

/// A sensor Home Assistant should make from a system's state topic.
struct Sensor {
    key: &'static str,
    name: &'static str,
    template: &'static str,
    unit: Option<&'static str>,
    class: Option<&'static str>,
    state_class: Option<&'static str>,
}

const SENSORS: [Sensor; 7] = [
    Sensor {
        key: "decode_rate",
        name: "Decode rate",
        template: "{{ value_json.decode_rate }}",
        unit: Some("msg/s"),
        class: None,
        state_class: Some("measurement"),
    },
    Sensor { key: "active_calls", name: "Active calls", template: "{{ value_json.active_calls }}", unit: None, class: None, state_class: Some("measurement") },
    Sensor { key: "recording", name: "Recording", template: "{{ value_json.recording }}", unit: None, class: None, state_class: Some("measurement") },
    Sensor { key: "calls_total", name: "Calls", template: "{{ value_json.calls_total }}", unit: None, class: None, state_class: Some("total_increasing") },
    Sensor {
        key: "recorded_total",
        name: "Recorded calls",
        template: "{{ value_json.recorded_total }}",
        unit: None,
        class: None,
        state_class: Some("total_increasing"),
    },
    Sensor {
        key: "audio_seconds_total",
        name: "Audio recorded",
        template: "{{ value_json.audio_seconds_total }}",
        unit: Some("s"),
        class: Some("duration"),
        state_class: Some("total_increasing"),
    },
    Sensor {
        key: "control_channel",
        name: "Control channel",
        template: "{{ value_json.control_channel_mhz if value_json.control_channel_mhz is number else '' }}",
        unit: Some("MHz"),
        class: Some("frequency"),
        state_class: None,
    },
];

/// Home Assistant's MQTT discovery for one system: a device with its sensors
/// and a "control channel locked" indicator. `node` names the recorder.
pub fn discovery(prefix: &str, discovery_prefix: &str, node: &str, system: &str, label: &str) -> Vec<Message> {
    let id = format!("{}_{}", level(node), level(system));
    let device = json!({ "identifiers": [id.clone()], "name": format!("Trunk Recorder Pro {label}"), "manufacturer": "Trunk Recorder Pro", "model": "Trunked radio system" });
    let state = state_topic(prefix, system);
    let avail = status_topic(prefix);
    let mut out = Vec::new();
    for s in &SENSORS {
        let mut p = json!({
            "name": s.name,
            "unique_id": format!("{id}_{}", s.key),
            "state_topic": state,
            "value_template": s.template,
            "availability_topic": avail,
            "device": device,
        });
        for (k, v) in [("unit_of_measurement", s.unit), ("device_class", s.class), ("state_class", s.state_class)] {
            if let Some(v) = v {
                p[k] = json!(v);
            }
        }
        out.push(Message { topic: format!("{discovery_prefix}/sensor/{id}/{}/config", s.key), payload: p.to_string(), retain: true, event: false });
    }
    let p = json!({
        "name": "Control channel locked",
        "unique_id": format!("{id}_locked"),
        "state_topic": state,
        "value_template": "{{ 'ON' if value_json.locked == 1 else 'OFF' }}",
        "device_class": "connectivity",
        "availability_topic": avail,
        "device": device,
    });
    out.push(Message { topic: format!("{discovery_prefix}/binary_sensor/{id}/locked/config"), payload: p.to_string(), retain: true, event: false });
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use trunk_recorder_plugin::{CallRecord, SrcEntry};

    fn info(recording: bool, reason: Option<&str>, encrypted: bool) -> CallInfo {
        CallInfo { id: 1, short_name: "county".into(), talkgroup: 101, recording, reason: reason.map(String::from), encrypted, ..Default::default() }
    }

    #[test]
    fn counts_calls_by_what_became_of_them() {
        let mut n = Counts::default();
        n.started(&info(true, None, false));
        n.started(&info(false, Some("encrypted"), true));
        n.started(&info(false, Some("no_recorder"), false));
        n.started(&info(false, None, false));
        assert_eq!((n.calls, n.encrypted), (4, 1));
        assert_eq!(n.not_recorded["encrypted"], 1);
        assert_eq!(n.not_recorded["no_recorder"], 1);
        assert_eq!(n.not_recorded["other"], 1);
    }

    #[test]
    fn topic_levels_cannot_break_out() {
        assert_eq!(level("county/fire #1+"), "county_fire__1_");
        assert_eq!(level(""), "_");
        assert_eq!(state_topic("trunk-pro", "a b"), "trunk-pro/a_b/state");
        assert_eq!(call_topic("trunk-pro", "county"), "trunk-pro/county/call");
    }

    #[test]
    fn state_says_unlocked_until_it_has_a_control_channel() {
        let n = Counts::default();
        let s = SystemStatus { short_name: "county".into(), control_channel_hz: None, decode_rate: 0.0, ..Default::default() };
        let v = state(1.0, &s, &n);
        assert_eq!(v["locked"], 0);
        assert!(v["control_channel_mhz"].is_null());
        let s = SystemStatus { control_channel_hz: Some(851_012_500), decode_rate: 38.5, active_calls: 3, recording: 2, ..s };
        let v = state(2.0, &s, &n);
        assert_eq!((v["locked"].clone(), v["control_channel_mhz"].clone()), (json!(1), json!(851.0125)));
        assert_eq!((v["active_calls"].clone(), v["recording"].clone(), v["decode_rate"].clone()), (json!(3), json!(2), json!(38.5)));
    }

    fn concluded() -> ConcludedCall {
        let mut call = CallRecord {
            talkgroup: 101,
            talkgroup_tag: "Fire".into(),
            call_length: 4.2,
            freq: 857_587_500,
            start_time: 1000,
            encrypted: true,
            ..Default::default()
        };
        call.src_list = vec![SrcEntry { src: 7, ..Default::default() }, SrcEntry { src: 7, ..Default::default() }, SrcEntry { src: 9, ..Default::default() }];
        call.extra.insert("snr".into(), json!(24.5));
        call.extra.insert("clean_voice_pct".into(), Value::Null);
        call.extra.insert("decrypted".into(), json!(1));
        ConcludedCall { path: "county/x".into(), call, ..Default::default() }
    }

    #[test]
    fn a_call_event_carries_what_was_measured() {
        let v = call("County", &concluded());
        assert_eq!((v["system"].clone(), v["talkgroup"].clone(), v["length_s"].clone()), (json!("County"), json!(101), json!(4.2)));
        assert_eq!((v["units"].clone(), v["unit_count"].clone()), (json!([7, 9]), json!(2)));
        assert_eq!(v["snr"], 24.5);
        assert!(v.get("clean_voice_pct").is_none(), "a null measurement is left out");
        assert_eq!(v["decrypted"], 1);
        assert_eq!(v["encrypted"], 1, "0 or 1, as Trunk Recorder writes them (Telegraf drops booleans)");
    }

    #[test]
    fn totals_follow_concluded_calls() {
        let mut n = Counts::default();
        n.concluded(&concluded());
        n.concluded(&concluded());
        assert_eq!((n.recorded, n.audio_seconds), (2, 8.4));
        assert_eq!(n.last_call.as_ref().unwrap()["talkgroup"], 101);
        let s = SystemStatus::default();
        assert_eq!(state(0.0, &s, &n)["audio_seconds_total"], 8.4);
    }

    #[test]
    fn discovery_makes_a_device_of_sensors_tied_to_the_status_topic() {
        let m = discovery("trunk-pro", "homeassistant", "my recorder", "county", "County");
        assert_eq!(m.len(), SENSORS.len() + 1);
        assert!(m.iter().all(|m| m.retain && m.topic.starts_with("homeassistant/") && m.topic.ends_with("/config")));
        let rate: Value = serde_json::from_str(&m.iter().find(|m| m.topic.ends_with("/decode_rate/config")).unwrap().payload).unwrap();
        assert_eq!(rate["state_topic"], "trunk-pro/county/state");
        assert_eq!(rate["availability_topic"], "trunk-pro/status");
        assert_eq!(rate["unique_id"], "my_recorder_county_decode_rate");
        assert_eq!(rate["device"]["identifiers"][0], "my_recorder_county");
        assert_eq!(rate["unit_of_measurement"], "msg/s");
        let ids: BTreeSet<_> = m.iter().map(|m| serde_json::from_str::<Value>(&m.payload).unwrap()["unique_id"].as_str().unwrap().to_string()).collect();
        assert_eq!(ids.len(), m.len(), "every unique id differs");
    }
}
