# MQTT statistics plugin for Trunk Recorder Pro

Publishes each system's state and every recorded call to an MQTT broker, so they can go to
**Home Assistant** (with auto-discovery) and to **Grafana** (through Telegraf's `mqtt_consumer`
into InfluxDB), or to anything else that reads MQTT.

It only watches: it keeps no call files (only a small list of the retained topics it published, in its data folder), never holds up the recorder, and reports every concluded call
as skipped, so nothing waits on it. If the broker goes away the plugin reconnects by itself, and
the recorder records as usual.

## Install

There is no published release yet, so build it (`cargo build --release`) and, in Trunk Recorder
Pro's Setup → Plugins, add a plugin that runs the executable (`target/release/mqtt`; the plugin's
**path** setting). Fill in the broker and turn it on. Once it's released, it can be installed from
its GitHub release (`connorjfarrell/trunk-plugin-mqtt`).

## Settings

| Setting | Default | |
|---|---|---|
| Broker host, Port | `localhost`, `1883` | `8883` is the usual TLS port |
| Use TLS | off | |
| Username, Password | empty | Leave empty for a broker with no login |
| Client ID | `trunk-pro` | Names this recorder to the broker and in Home Assistant's device ids; use a different one per recorder |
| Topic prefix | `trunk-pro` | May have levels (`radio/trunk`); no wildcards |
| Keep the latest state on the broker | on | Retained: a program that connects later sees each system's last state at once |
| Quality of service | 0 | 0 or 1 |
| Publish every call | on | The per-call stream below |
| Home Assistant discovery, Discovery prefix | off, `homeassistant` | |

Each system can be left out, or given a name for topics and Home Assistant (its short name otherwise).

## What it publishes

`<p>` is the topic prefix and `<s>` the system's name (`/`, `+`, `#` and spaces become `_`).

| Topic | Payload |
|---|---|
| `<p>/status` | `online`, or `offline` (the broker sends it if the plugin vanishes). Retained |
| `<p>/<s>/state` | JSON every 5 seconds. Retained unless turned off |
| `<p>/<s>/call` | JSON for each recorded call, when it's saved. Never retained |

`state`:

```json
{
  "time": 1790771560.0,
  "control_channel_hz": 851012500, "control_channel_mhz": 851.0125, "locked": 1,
  "decode_rate": 38.5, "active_calls": 3, "recording": 2,
  "calls_total": 412, "recorded_total": 380, "encrypted_total": 20,
  "not_recorded_total": 32, "not_recorded": { "encrypted": 20, "no_recorder": 12 },
  "audio_seconds_total": 5120.4,
  "last_call": { "time": 1790771550, "talkgroup": 101, "tag": "Fire Dispatch", "length_s": 4.0 }
}
```

`locked` is 1 once the system has a control channel (`control_channel_hz` is `null` while it
searches). The `_total` figures count from when the plugin started, so they restart from zero with
the recorder: use a counter's *rate* or *increase*, not its value. `not_recorded` counts calls the
recorder followed but didn't record, by its reason (`encrypted`, `unknown_tg`, `no_recorder`,
`no_source`, …).

`call`:

```json
{
  "time": 1790771550, "system": "County", "talkgroup": 101, "tag": "Fire Dispatch", "group": "Fire",
  "freq_hz": 857587500, "length_s": 4.0, "audio_type": "digital", "tdma_slot": 0,
  "emergency": 0, "encrypted": 0, "units": [1116707], "unit_count": 1,
  "error_count": 0, "spike_count": 0, "snr": 24.5, "signal": -38.2, "noise": -62.7, "clean_voice_pct": 99.1
}
```

`snr`, `signal`, `noise`, `clean_voice_pct`, `freq_error` and `decrypted` are there when the recorder
measured them. Flags are `0` or `1`, as in Trunk Recorder's call JSON (Telegraf drops JSON booleans).

## Home Assistant

Turn on **Home Assistant discovery** (your broker is the one Home Assistant's MQTT integration uses).
Each system appears as a device, **Trunk Recorder Pro `<name>`**, with sensors for decode rate, active
calls, recording, calls, recorded calls, audio recorded and control channel, and a *Control channel
locked* indicator. They go unavailable when `<p>/status` is `offline`.

## Grafana, through Telegraf

Telegraf reads the topics and writes InfluxDB; Grafana reads InfluxDB. This is the Telegraf side:

```toml
[[inputs.mqtt_consumer]]
  servers = ["tcp://localhost:1883"]
  topics = ["trunk-pro/+/state"]
  name_override = "trunkpro_state"
  data_format = "json"
  json_time_key = "time"
  json_time_format = "unix"
  [[inputs.mqtt_consumer.topic_parsing]]
    topic = "trunk-pro/+/state"
    tags = "_/system/_"

[[inputs.mqtt_consumer]]
  servers = ["tcp://localhost:1883"]
  topics = ["trunk-pro/+/call"]
  name_override = "trunkpro_call"
  data_format = "json"
  tag_keys = ["talkgroup", "tag", "group", "audio_type", "tdma_slot"]
  json_time_key = "time"
  json_time_format = "unix"
  [[inputs.mqtt_consumer.topic_parsing]]
    topic = "trunk-pro/+/call"
    tags = "_/system/_"
```

That gives a `trunkpro_state` measurement (one point per system every 5 seconds: `decode_rate`,
`active_calls`, `calls_total`, …) and a `trunkpro_call` measurement (one point per call, tagged by
system, talkgroup and tag: `length_s`, `snr`, `clean_voice_pct`, …). Some panels to start from:
calls per talkgroup (`count` of `trunkpro_call` grouped by `tag`), airtime (`sum(length_s)`),
control-channel decode rate over time, and `clean_voice_pct` by system.

## Building and testing

```sh
cargo test
# against a real broker (Mosquitto, say):
MQTT_TEST_ADDR=127.0.0.1:1883 cargo test -- --ignored
```

The message building (`src/messages.rs`, `src/report.rs`) is separate from the network
(`src/link.rs`), and is tested with a recording publisher.

## License

MIT.
