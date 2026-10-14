//! Turns the recorder's events into messages: what each system has done is
//! counted here, and every status tick publishes its state.

use std::collections::HashMap;

use trunk_recorder_plugin::{CallInfo, ConcludedCall, Status};

use crate::messages::{self, Counts, Message, Publisher};

/// How a system appears: its topic name, or left out.
#[derive(Clone, Debug)]
pub struct SystemLabel {
    pub short_name: String,
    /// What the user called it (the short name when they didn't).
    pub label: String,
    pub skip: bool,
}

#[derive(Clone)]
pub struct Options {
    pub prefix: String,
    /// Retain each system's state (the per-call events never are).
    pub retain: bool,
    pub publish_calls: bool,
    /// Home Assistant's discovery prefix, when discovery is on.
    pub discovery: Option<String>,
    /// Names the recorder in Home Assistant's device ids.
    pub node: String,
}

pub struct Reporter {
    opt: Options,
    /// Retained messages that empty discovery topics no longer wanted.
    cleanup: Vec<Message>,
    systems: Vec<SystemLabel>,
    counts: HashMap<String, Counts>,
    out: Box<dyn Publisher>,
}

impl Reporter {
    pub fn new(opt: Options, systems: Vec<SystemLabel>, out: Box<dyn Publisher>) -> Reporter {
        Reporter { opt, cleanup: Vec::new(), systems, counts: HashMap::new(), out }
    }

    /// Home Assistant's discovery messages for the systems kept (none when it's off).
    pub fn discovery(&self) -> Vec<Message> {
        let Some(d) = &self.opt.discovery else { return Vec::new() };
        self.systems.iter().filter(|s| !s.skip).flat_map(|s| messages::discovery(&self.opt.prefix, d, &self.opt.node, &s.label, &s.label)).collect()
    }

    /// Discovery topics an earlier run published that this one doesn't: say they're gone.
    pub fn clean_up(&mut self, previous: &[String]) {
        self.cleanup = stale_discovery(previous, &self.discovery());
    }

    /// Sent at every (re)connect: that the recorder is online, Home Assistant's discovery, and what to clear from it.
    pub fn on_connect(&self) -> Vec<Message> {
        let mut m = vec![Message { topic: messages::status_topic(&self.opt.prefix), payload: "online".into(), retain: true }];
        m.extend(self.discovery());
        m.extend(self.cleanup.iter().cloned());
        m
    }

    /// The system's label, or None when it's left out. A system the recorder
    /// didn't list at start (it can't happen, but) is kept under its short name.
    fn label(&self, short_name: &str) -> Option<String> {
        match self.systems.iter().find(|s| s.short_name == short_name) {
            Some(s) if s.skip => None,
            Some(s) => Some(s.label.clone()),
            None => Some(short_name.to_string()),
        }
    }

    pub fn status(&mut self, st: &Status) {
        for s in &st.systems {
            let Some(label) = self.label(&s.short_name) else { continue };
            let n = self.counts.entry(s.short_name.clone()).or_default();
            let payload = messages::state(st.time, s, n).to_string();
            self.out.publish(Message { topic: messages::state_topic(&self.opt.prefix, &label), payload, retain: self.opt.retain });
        }
    }

    pub fn call_start(&mut self, c: &CallInfo) {
        if self.label(&c.short_name).is_some() {
            self.counts.entry(c.short_name.clone()).or_default().started(c);
        }
    }

    pub fn call_concluded(&mut self, c: &ConcludedCall) {
        let Some(label) = self.label(&c.call.short_name) else { return };
        self.counts.entry(c.call.short_name.clone()).or_default().concluded(c);
        if self.opt.publish_calls {
            let payload = messages::call(&label, c).to_string();
            self.out.publish(Message { topic: messages::call_topic(&self.opt.prefix, &label), payload, retain: false });
        }
    }
}

/// Messages that empty (retained) the topics in `previous` that `current` no longer has.
pub fn stale_discovery(previous: &[String], current: &[Message]) -> Vec<Message> {
    previous.iter().filter(|t| !current.iter().any(|m| &m.topic == *t)).map(|t| Message { topic: t.clone(), payload: String::new(), retain: true }).collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use trunk_recorder_plugin::{CallRecord, SystemStatus};

    use super::*;

    #[derive(Clone, Default)]
    pub struct Recorded(pub Arc<Mutex<Vec<Message>>>);

    impl Publisher for Recorded {
        fn publish(&self, m: Message) {
            self.0.lock().unwrap().push(m);
        }
    }

    fn reporter(publish_calls: bool, discovery: bool) -> (Reporter, Recorded) {
        let out = Recorded::default();
        let systems = vec![
            SystemLabel { short_name: "county".into(), label: "County".into(), skip: false },
            SystemLabel { short_name: "city".into(), label: "city".into(), skip: true },
        ];
        let opt = Options { prefix: "tr".into(), retain: true, publish_calls, discovery: discovery.then(|| "homeassistant".into()), node: "rec".into() };
        (Reporter::new(opt, systems, Box::new(out.clone())), out)
    }

    fn status() -> Status {
        let sys = |n: &str| SystemStatus {
            short_name: n.into(),
            control_channel_hz: Some(851_000_000),
            decode_rate: 40.0,
            active_calls: 1,
            recording: 1,
            ..Default::default()
        };
        Status { time: 5.0, systems: vec![sys("county"), sys("city")] }
    }

    fn concluded(system: &str) -> ConcludedCall {
        ConcludedCall {
            path: "p".into(),
            call: CallRecord { short_name: system.into(), talkgroup: 101, call_length: 3.0, ..Default::default() },
            ..Default::default()
        }
    }

    #[test]
    fn a_status_tick_publishes_each_systems_state_retained_under_its_label() {
        let (mut r, out) = reporter(true, false);
        r.status(&status());
        let m = out.0.lock().unwrap();
        assert_eq!(m.len(), 1, "the system left out is silent");
        assert_eq!((m[0].topic.as_str(), m[0].retain), ("tr/County/state", true));
        let v: Value = serde_json::from_str(&m[0].payload).unwrap();
        assert_eq!((v["decode_rate"].clone(), v["calls_total"].clone()), (json!(40.0), json!(0)));
    }

    #[test]
    fn calls_are_counted_and_published_unretained() {
        let (mut r, out) = reporter(true, false);
        r.call_start(&CallInfo { short_name: "county".into(), recording: true, ..Default::default() });
        r.call_concluded(&concluded("county"));
        r.call_concluded(&concluded("city"));
        r.status(&status());
        let m = out.0.lock().unwrap();
        assert_eq!(m.len(), 2, "one call event, one state; nothing for the system left out");
        assert_eq!((m[0].topic.as_str(), m[0].retain), ("tr/County/call", false));
        let v: Value = serde_json::from_str(&m[1].payload).unwrap();
        assert_eq!((v["calls_total"].clone(), v["recorded_total"].clone(), v["audio_seconds_total"].clone()), (json!(1), json!(1), json!(3.0)));
    }

    #[test]
    fn call_events_can_be_turned_off() {
        let (mut r, out) = reporter(false, false);
        r.call_concluded(&concluded("county"));
        assert!(out.0.lock().unwrap().is_empty());
        r.status(&status());
        let v: Value = serde_json::from_str(&out.0.lock().unwrap()[0].payload).unwrap();
        assert_eq!(v["recorded_total"], 1, "still counted");
    }

    #[test]
    fn on_connect_says_online_and_adds_discovery_only_when_asked() {
        let (r, _) = reporter(true, false);
        let m = r.on_connect();
        assert_eq!(m, vec![Message { topic: "tr/status".into(), payload: "online".into(), retain: true }]);
        let (r, _) = reporter(true, true);
        let m = r.on_connect();
        assert!(m.len() > 1 && m.iter().skip(1).all(|m| m.topic.starts_with("homeassistant/") && m.topic.contains("rec_County")), "{m:?}");
    }

    #[test]
    fn discovery_no_longer_wanted_is_emptied() {
        let (mut r, _) = reporter(true, true);
        let now: Vec<String> = r.discovery().into_iter().map(|m| m.topic).collect();
        assert!(!now.is_empty());
        // Nothing changed: nothing to clear.
        r.clean_up(&now);
        assert_eq!(r.on_connect().len(), 1 + now.len());
        // A topic an earlier run had, and one this run has.
        let mut before = now.clone();
        before.push("homeassistant/sensor/rec_Old/decode_rate/config".into());
        r.clean_up(&before);
        let m = r.on_connect();
        assert_eq!(m.len(), 2 + now.len());
        assert_eq!(m.last().unwrap(), &Message { topic: "homeassistant/sensor/rec_Old/decode_rate/config".into(), payload: String::new(), retain: true });
        // Discovery turned off: all of it is cleared.
        let (mut off, _) = reporter(true, false);
        off.clean_up(&now);
        let m = off.on_connect();
        assert_eq!(m.len(), 1 + now.len());
        assert!(m.iter().skip(1).all(|m| m.payload.is_empty() && m.retain));
    }
}
