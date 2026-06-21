#!/usr/bin/env python3
"""Urban Arrow (Bosch eBike) battery -> MQTT reader for Home Assistant. v1.1.

Use case: you arrive home, park the bike near this host; the battery % is read.

The host must be BONDED + TRUSTED with the bike (pair once with bluetoothctl).
A persistent scanner waits for the bike to advertise; on detection it connects,
reads the eb21 telemetry snapshot, publishes the battery % to MQTT, and
disconnects. The value is published RETAINED with no availability topic, so the
last reading (plus a "last updated" timestamp) stays visible in Home Assistant
until a new reading arrives — it never goes "unavailable". The on-disk bond
survives reboots.

Device selection:
- Set BIKE_ADDRESS to pin a specific bike, OR leave it empty to AUTO-DETECT the
  Bosch hub by its advertised name ("smart system eBike"). All candidates are
  logged, and the selected BLE address is published as a diagnostic sensor.

Config via environment variables (see the add-on / systemd unit):
  BIKE_ADDRESS (empty = auto), MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS,
  COOLDOWN, OP_TIMEOUT
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time

import paho.mqtt.client as mqtt
from bleak import BleakClient, BleakScanner

try:
    from aiohttp import web  # setup UI (Ingress)
except Exception:  # noqa: BLE001 - optional; reader still works without the UI
    web = None  # type: ignore[assignment]

ADDRESS = os.getenv("BIKE_ADDRESS", "").strip()
AUTO = ADDRESS == ""
NAME_MATCH = "smart system"  # Bosch Smart System hub advertised name
EB21 = "0000eb21-eaa2-11e9-81b4-2a2ae2dbcce4"
EB41 = "0000eb41-eaa2-11e9-81b4-2a2ae2dbcce4"  # config: mode list + frame number
FIELD_BATTERY = 10

# Standard BLE Device Information Service (0x180A) — read once when connected.
DEVICE_INFO_CHARS = {
    "manufacturer": "00002a29-0000-1000-8000-00805f9b34fb",
    "model": "00002a24-0000-1000-8000-00805f9b34fb",
    "serial": "00002a25-0000-1000-8000-00805f9b34fb",
    "firmware": "00002a26-0000-1000-8000-00805f9b34fb",
    "hardware": "00002a27-0000-1000-8000-00805f9b34fb",
}

# Bosch push channel: notifications carry the live-selected ride mode.
PUSH_NOTIFY = "00000011-eaa2-11e9-81b4-2a2ae2dbcce4"
PUSH_WRITE = "00000012-eaa2-11e9-81b4-2a2ae2dbcce4"
MODE_NAMES = {1: "Eco", 2: "Tour", 3: "Auto", 4: "Turbo"}

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")

COOLDOWN = float(os.getenv("COOLDOWN", "120"))
OP_TIMEOUT = float(os.getenv("OP_TIMEOUT", "15"))
SCAN_GAP = float(os.getenv("SCAN_GAP", "3"))

# COMODULE (URBANARROW) motion tracker — always-on, own battery.
COMODULE_ADDRESS = os.getenv("COMODULE_ADDRESS", "").strip()
COMODULE_NAME = "urbanarrow"
CHAR_155E = "0000155e-1212-efde-1523-785feabcd123"
FRAME_MOTION = 0xD1  # 155e frame type that floods while the bike is moved
MOTION_OFF_DELAY = float(os.getenv("MOTION_OFF_DELAY", "12"))

DISC_PREFIX = "homeassistant"
NODE = "urban_arrow"
STATE_TOPIC = f"{NODE}/state"
STATUS_TOPIC = f"{NODE}/status"
MODE_TOPIC = f"{NODE}/mode"
RANGE_TOPIC = f"{NODE}/range"
MOTION_TOPIC = f"{NODE}/motion"
TRACKER_TOPIC = f"{NODE}/tracker"
ALARM_STATE_TOPIC = f"{NODE}/alarm/state"
ALARM_CMD_TOPIC = f"{NODE}/alarm/cmd"
LOCK_STATE_TOPIC = f"{NODE}/lock/state"
LOCK_CMD_TOPIC = f"{NODE}/lock/cmd"
ARMED_STATES = ("armed_away", "armed_home", "armed_night")

_mqtt: "mqtt.Client | None" = None
# Alarm state machine (HomeKit Security System via MQTT alarm_control_panel).
_alarm: dict[str, object] = {"state": "disarmed", "restored": False, "fired": False}
# eBike Lock: a pending ON/OFF command applied on the next bike connection, and a
# per-write apply-trigger sequence counter (8d2f). ON = write 8d1c 0x40 (+ apply);
# OFF = write 8d1c 0x00 (inferred — the app omits 8d1c when off). Bike must be awake.
_lock_cmd: "str | None" = None
_lock_seq: int = 0x30
# Auto-detect: lock onto the first bike we successfully read, and back off bikes
# we fail to pair with (neighbours' "smart system eBike"s), to avoid churn.
_locked_addr: "str | None" = None
_pair_fail: dict[str, float] = {}
PAIR_RETRY_AFTER = 3600.0

# ---------------------------------------------- setup UI: config + state
DATA_FILE = "/data/ua.json"
INGRESS_PORT = int(os.getenv("INGRESS_PORT", "8099"))


def _load_cfg() -> dict:
    try:
        with open(DATA_FILE) as fh:
            return json.load(fh)
    except Exception:  # noqa: BLE001
        return {}


def _save_cfg() -> None:
    try:
        with open(DATA_FILE, "w") as fh:
            json.dump({"bike": _bike_addr, "tracker": _tracker_mac,
                       "tracker_off": _tracker_off, "alarm_off": _alarm_off,
                       "bike_model": _last.get("bike_model"),
                       "battery_model": _last.get("battery_model")}, fh)
    except Exception as err:  # noqa: BLE001
        log.warning("save config: %s", err)


_cfg0 = _load_cfg()
# Bike BLE address to lock onto ("" = auto-detect by name). /data wins over env.
_bike_addr: "str | None" = (_cfg0.get("bike") or ADDRESS or "").strip() or None
# Tracker fixed module MAC (from the advertisement) — robust against its rotating
# BLE address. "" = auto-detect the first URBANARROW.
_tracker_mac: "str | None" = (_cfg0.get("tracker") or COMODULE_ADDRESS or "").strip() or None
_tracker_off: bool = bool(_cfg0.get("tracker_off", False))
# Alarm (HomeKit Security System) is optional on top of the motion sensor.
_alarm_off: bool = bool(_cfg0.get("alarm_off", False))
# Battery-friendly: only hold the tracker connection while the alarm is armed
# (it has its own battery; a permanent connection drains it). Set true to keep
# it connected always (live motion even when disarmed).
_tracker_always: bool = os.getenv("TRACKER_ALWAYS", "0") == "1"

# Devices seen during scans, for the setup UI: address -> {name,rssi,kind,module_mac,ts}.
_discovered: dict[str, dict] = {}
# Last known values, for the setup UI status panel.
_last: dict[str, object] = {}
# Marketing model name is NOT broadcast over BLE (it lives in the Urban Arrow
# account/cloud), so it is config-driven: the optional bike_model option wins,
# otherwise default to "Urban Arrow Connected".
_model_name: str = (os.getenv("BIKE_MODEL", "").strip()
                    or _cfg0.get("bike_model") or "Urban Arrow Connected")
_last["bike_model"] = _model_name
if _cfg0.get("battery_model"):
    _last["battery_model"] = _cfg0["battery_model"]
# Serialise BLE scans: the reader loop, tracker locate, and UI scans must not run
# a BleakScanner simultaneously (org.bluez "Operation already in progress").
_scan_lock = asyncio.Lock()


def publish_status(status: str, present: str = "ON") -> None:
    """Publish a human-readable status + present(ON/OFF) for the status sensors."""
    if _mqtt is not None:
        _mqtt.publish(STATUS_TOPIC, json.dumps({"status": status, "present": present}),
                      retain=True)


def publish_motion(on: bool) -> None:
    """Publish the motion binary_sensor state (retained)."""
    if _mqtt is not None:
        _mqtt.publish(MOTION_TOPIC, "ON" if on else "OFF", retain=True)


def publish_alarm(state: str) -> None:
    """Publish the alarm_control_panel state (retained)."""
    _last["alarm"] = state
    if _mqtt is not None:
        _mqtt.publish(ALARM_STATE_TOPIC, state, retain=True)


def publish_lock(state: str) -> None:
    """Publish the eBike Lock switch state (ON/OFF, retained)."""
    _last["lock"] = state
    if _mqtt is not None:
        _mqtt.publish(LOCK_STATE_TOPIC, state, retain=True)


def _want_tracker() -> bool:
    """Whether to hold the tracker connection now. Battery-friendly: only while
    armed, unless tracker_always is set (and never if the tracker is disabled)."""
    if _tracker_off:
        return False
    if _tracker_always:
        return True
    return not _alarm_off and _alarm["state"] in (ARMED_STATES + ("triggered",))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bosch-reader")


# ---------------------------------------------------------------- protobuf
def _pb_fields(raw: bytes) -> dict[int, object]:
    """Decode a flat protobuf into {field_number: int | bytes}."""
    fields: dict[int, object] = {}
    pos = 0

    def rv(d: bytes, p: int) -> tuple[int, int]:
        result = shift = 0
        while p < len(d):
            b = d[p]
            p += 1
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        return result, p

    while pos < len(raw):
        try:
            tag, pos = rv(raw, pos)
            fn, wt = tag >> 3, tag & 7
            if wt == 0:
                fields[fn], pos = rv(raw, pos)
            elif wt == 2:
                ln, pos = rv(raw, pos)
                fields[fn] = raw[pos:pos + ln]
                pos += ln
            elif wt == 5:
                fields[fn] = int.from_bytes(raw[pos:pos + 4], "little")
                pos += 4
            elif wt == 1:
                fields[fn] = int.from_bytes(raw[pos:pos + 8], "little")
                pos += 8
            else:
                break
        except Exception:  # noqa: BLE001
            break
    return fields


def parse_eb21(raw: bytes) -> dict[str, int]:
    """Decode the eb21 snapshot. Known fields:
      f10 = battery %, f12 = odometer (metres),
      f20.f2 = odometer at which the next service is due (metres).
    Returns battery, odometer (km) and next_service (km remaining) when present.
    """
    f = _pb_fields(raw)
    out: dict[str, int] = {}
    if isinstance(f.get(FIELD_BATTERY), int):
        out["battery"] = f[FIELD_BATTERY]  # type: ignore[index]
    odo = f.get(12)
    if isinstance(odo, int):
        out["odometer"] = odo // 1000
        target = _pb_fields(f[20]).get(2) if isinstance(f.get(20), bytes) else None
        if isinstance(target, int):
            out["next_service"] = (target - odo) // 1000
    return out


def parse_eb41_frame(raw: bytes) -> str | None:
    """eb41 field 9 carries the frame number string, e.g. '2508179RFGP'."""
    v = _pb_fields(raw).get(9)
    if isinstance(v, (bytes, bytearray)) and len(v) >= 6 and all(32 <= c < 127 for c in v):
        return bytes(v).decode()
    return None


# Component subsystem per record attribute high byte (push 0x001e records:
# 30 LL <attr:2> c0 80 10 0a <slen> <ascii>). Each subsystem reports its own
# firmware (a 19.x.x string) and production date (dd.mm.yyyy).
_COMP_GROUP = {0x20: "controller", 0x18: "drive", 0x00: "battery", 0x0d: "display"}
_DATE_RE = re.compile(r"^\d\d\.\d\d\.\d{4}$")
_FW_RE = re.compile(r"^19\.\d+\.\d+$")


def parse_components(buf: bytes) -> dict:
    """Group the Bosch component-info strings by subsystem and pull each one's
    headline firmware (19.x.x) and production date. Returns
    {subsystem: {firmware, date}} — names come from content matches elsewhere."""
    by: dict[str, list[str]] = {}
    i = 0
    while i < len(buf) - 1:
        if buf[i] != 0x30:
            i += 1
            continue
        ln = buf[i + 1]
        rec = buf[i + 2:i + 2 + ln]
        i += 2 + ln
        if len(rec) < 3:
            continue
        m = rec.find(b"\xc0\x80\x10\x0a")  # string marker
        if m < 1:
            continue
        slen = rec[m + 4] if m + 4 < len(rec) else 0
        s = rec[m + 5:m + 5 + slen]
        if not s or not all(32 <= c < 127 for c in s):
            continue
        grp = _COMP_GROUP.get(rec[0])
        if grp:
            by.setdefault(grp, []).append(s.decode())
    out: dict[str, dict] = {}
    for grp, strs in by.items():
        fw = next((x for x in strs if _FW_RE.match(x)), None)
        date = next((x for x in strs if _DATE_RE.match(x)), None)
        if fw or date:
            out[grp] = {"firmware": fw, "date": date}
    return out


def parse_mode(raw: bytes) -> str | None:
    """Return the ride mode from a Bosch push-channel notification, or None.

    On record:  ... 98 09 08 <level> ...  (level 1=Eco 2=Tour 3=Auto 4=Turbo)
    Off record: ... 30 02 98 09 ...        (a length-2 record, no level byte)
    """
    i = raw.find(b"\x98\x09")
    while i != -1:
        if i + 3 < len(raw) and raw[i + 2] == 0x08 and raw[i + 3] in MODE_NAMES:
            return MODE_NAMES[raw[i + 3]]
        if i >= 2 and raw[i - 2] == 0x30 and raw[i - 1] == 0x02:
            return "Off"
        i = raw.find(b"\x98\x09", i + 2)
    return None


def parse_range(raw: bytes) -> dict[str, int] | None:
    """Return the estimated range (km) per mode from a push notification.

    Attribute 9857 carries a 4-byte array `98 57 0a 04 <eco><tour><auto><turbo>`
    (ascending assist order, each a km value).
    """
    i = raw.find(b"\x98\x57\x0a\x04")
    if i != -1 and i + 8 <= len(raw):
        a = raw[i + 4:i + 8]
        return {"eco": a[0], "tour": a[1], "auto": a[2], "turbo": a[3]}
    return None


# ------------------------------------------------------------------- MQTT
DEVICE = {
    "identifiers": [NODE],
    "name": "Urban Arrow",
    "manufacturer": "Bosch eBike Systems",
    "model": "Smart System (BRC3600)",
}


def _on_connect(client, _userdata, _flags, reason, _properties=None):
    rc = getattr(reason, "value", reason)
    if rc != 0:
        log.error("MQTT connection REFUSED (reason=%s) — check MQTT_USER/MQTT_PASS", reason)
        return
    _publish_discovery(client)
    # Alarm: receive HomeKit/HA arm/disarm commands + restore the retained state.
    client.subscribe(ALARM_CMD_TOPIC)
    client.subscribe(ALARM_STATE_TOPIC)
    client.subscribe(LOCK_CMD_TOPIC)
    client.subscribe(LOCK_STATE_TOPIC)
    # Restore the last measurement (retained) so the UI shows it after a restart.
    client.subscribe(STATE_TOPIC)
    client.subscribe(MODE_TOPIC)
    client.subscribe(RANGE_TOPIC)
    client.subscribe(MOTION_TOPIC)
    client.subscribe(TRACKER_TOPIC)
    log.info("connected to MQTT %s:%s", MQTT_HOST, MQTT_PORT)


def _on_message(_client, _userdata, msg) -> None:
    try:
        payload = msg.payload.decode(errors="ignore").strip()
    except Exception:  # noqa: BLE001
        return
    if msg.topic == ALARM_CMD_TOPIC:
        new = {"DISARM": "disarmed", "ARM_AWAY": "armed_away",
               "ARM_HOME": "armed_home", "ARM_NIGHT": "armed_night"}.get(payload.upper())
        if new:
            _alarm["state"] = new
            _alarm["fired"] = False  # allow a fresh trigger after (re)arm/disarm
            publish_alarm(new)
            log.info("alarm command %s -> %s", payload, new)
    elif msg.topic == LOCK_CMD_TOPIC:
        global _lock_cmd
        cmd = payload.upper()
        if cmd in ("ON", "OFF"):
            _lock_cmd = cmd  # applied on the next bike connection (bike must be awake)
            log.info("lock command queued: %s", cmd)
    elif msg.topic == LOCK_STATE_TOPIC and "lock" not in _last:
        if payload in ("ON", "OFF"):
            _last["lock"] = payload  # restore retained state on reconnect
    elif msg.topic == ALARM_STATE_TOPIC and not _alarm["restored"]:
        # First retained message after (re)connect = restore the previous state.
        _alarm["restored"] = True
        if payload in ARMED_STATES + ("disarmed", "triggered"):
            _alarm["state"] = payload
            _last["alarm"] = payload
            log.info("alarm state restored: %s", payload)
    elif msg.topic == STATE_TOPIC:        # retained last reading -> show in the UI
        try:
            _last.update(json.loads(payload))
        except Exception:  # noqa: BLE001
            pass
    elif msg.topic == MODE_TOPIC:
        try:
            _last["mode"] = json.loads(payload).get("mode")
        except Exception:  # noqa: BLE001
            pass
    elif msg.topic == RANGE_TOPIC:
        try:
            _last["range"] = json.loads(payload)
        except Exception:  # noqa: BLE001
            pass
    elif msg.topic == MOTION_TOPIC:
        _last["motion"] = payload == "ON"
    elif msg.topic == TRACKER_TOPIC:
        try:
            _last["tracker_battery"] = json.loads(payload).get("battery")
        except Exception:  # noqa: BLE001
            pass


def publish_alarm_discovery(client: mqtt.Client) -> None:
    """Publish (or remove, when disabled) the HomeKit alarm_control_panel."""
    topic = f"{DISC_PREFIX}/alarm_control_panel/{NODE}/alarm/config"
    if _alarm_off:
        client.publish(topic, "", retain=True)  # remove the accessory
        return
    client.publish(topic, json.dumps({
        "name": "Alarm",
        "unique_id": f"{NODE}_alarm",
        "state_topic": ALARM_STATE_TOPIC,
        "command_topic": ALARM_CMD_TOPIC,
        # Two meaningful modes for a bike: armed_away = loud, armed_home = silent.
        "supported_features": ["arm_away", "arm_home"],
        "code_arm_required": False,
        "code_disarm_required": False,
        "code_trigger_required": False,
        "icon": "mdi:shield-bike",
        "device": DEVICE,
    }), retain=True)


def _publish_discovery(client: mqtt.Client) -> None:
    # No availability_topic on purpose: the retained value stays shown (with its
    # timestamp) until the next reading — it never reports "unavailable".
    def cfg(obj_id: str, name: str, **extra) -> None:
        payload = {
            "name": name,
            "unique_id": f"{NODE}_{obj_id}",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.%s }}" % obj_id,
            "device": DEVICE,
            **extra,
        }
        client.publish(
            f"{DISC_PREFIX}/sensor/{NODE}/{obj_id}/config", json.dumps(payload), retain=True
        )

    cfg("battery", "Battery", device_class="battery",
        unit_of_measurement="%", state_class="measurement")
    cfg("last_updated", "Last updated", device_class="timestamp")
    cfg("address", "Bluetooth address", icon="mdi:bluetooth", entity_category="diagnostic")
    cfg("frame_number", "Frame number", icon="mdi:identifier", entity_category="diagnostic")
    cfg("part_number", "Part number", icon="mdi:barcode", entity_category="diagnostic")
    cfg("hub_firmware", "Hub firmware", icon="mdi:chip", entity_category="diagnostic")
    cfg("module_firmware", "Module firmware", icon="mdi:chip", entity_category="diagnostic")
    cfg("odometer", "Odometer", device_class="distance", unit_of_measurement="km",
        state_class="total_increasing", icon="mdi:counter")
    cfg("next_service", "Next service in", unit_of_measurement="km", icon="mdi:wrench-clock")

    # Ride mode sensor — reads MODE_TOPIC (its own retained topic so the last
    # known mode stays shown between rides).
    client.publish(
        f"{DISC_PREFIX}/sensor/{NODE}/mode/config",
        json.dumps({
            "name": "Ride mode",
            "unique_id": f"{NODE}_mode",
            "state_topic": MODE_TOPIC,
            "value_template": "{{ value_json.mode }}",
            "icon": "mdi:speedometer",
            "device": DEVICE,
        }),
        retain=True,
    )

    # Estimated range per mode (km) — reads RANGE_TOPIC.
    for key, label in (("eco", "Range Eco"), ("tour", "Range Tour+"),
                       ("auto", "Range Auto"), ("turbo", "Range Turbo")):
        client.publish(
            f"{DISC_PREFIX}/sensor/{NODE}/range_{key}/config",
            json.dumps({
                "name": label,
                "unique_id": f"{NODE}_range_{key}",
                "state_topic": RANGE_TOPIC,
                "value_template": "{{ value_json.%s }}" % key,
                "unit_of_measurement": "km",
                "icon": "mdi:map-marker-distance",
                "device": DEVICE,
            }),
            retain=True,
        )

    publish_alarm_discovery(client)

    # eBike Lock switch (writes to the bike's immobilizer; bike must be awake).
    client.publish(
        f"{DISC_PREFIX}/switch/{NODE}/lock/config",
        json.dumps({
            "name": "eBike Lock",
            "unique_id": f"{NODE}_lock",
            "command_topic": LOCK_CMD_TOPIC,
            "state_topic": LOCK_STATE_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "icon": "mdi:bike-fast",
            "device": DEVICE,
        }),
        retain=True,
    )

    # Status text sensor (current phase) — reads STATUS_TOPIC, not STATE_TOPIC.
    client.publish(
        f"{DISC_PREFIX}/sensor/{NODE}/status/config",
        json.dumps({
            "name": "Status",
            "unique_id": f"{NODE}_status",
            "state_topic": STATUS_TOPIC,
            "value_template": "{{ value_json.status }}",
            "icon": "mdi:bike",
            "entity_category": "diagnostic",
            "device": DEVICE,
        }),
        retain=True,
    )
    # Motion sensor — on while the COMODULE tracker reports movement (anti-theft).
    client.publish(
        f"{DISC_PREFIX}/binary_sensor/{NODE}/motion/config",
        json.dumps({
            "name": "Motion",
            "unique_id": f"{NODE}_motion",
            "state_topic": MOTION_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device_class": "motion",  # HomeKit maps this to a Motion Sensor
            "icon": "mdi:bike-fast",
            "device": DEVICE,
        }),
        retain=True,
    )

    # Tracker (COMODULE) own battery — drives the low-battery alarm cutoff.
    client.publish(
        f"{DISC_PREFIX}/sensor/{NODE}/tracker_battery/config",
        json.dumps({
            "name": "Tracker battery",
            "unique_id": f"{NODE}_tracker_battery",
            "state_topic": TRACKER_TOPIC,
            "value_template": "{{ value_json.battery }}",
            "device_class": "battery",
            "unit_of_measurement": "%",
            "entity_category": "diagnostic",
            "device": DEVICE,
        }),
        retain=True,
    )

    # Awake/reachable indicator — on = bike advertising (on/in range), off = not.
    client.publish(
        f"{DISC_PREFIX}/binary_sensor/{NODE}/awake/config",
        json.dumps({
            "name": "Awake",
            "unique_id": f"{NODE}_awake",
            "state_topic": STATUS_TOPIC,
            "value_template": "{{ value_json.present }}",
            "payload_on": "ON",
            "payload_off": "OFF",
            "device_class": "connectivity",
            "device": DEVICE,
        }),
        retain=True,
    )

    # Remove sensors published by earlier versions.
    for old in ("odometer", "battery2"):
        client.publish(f"{DISC_PREFIX}/sensor/{NODE}/{old}/config", "", retain=True)


def make_mqtt() -> mqtt.Client:
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="urban_arrow_reader")
    except AttributeError:  # paho-mqtt < 2.0
        client = mqtt.Client(client_id="urban_arrow_reader")
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = _on_connect
    client.on_message = _on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


# -------------------------------------------------------------------- bonding
async def _bctl(*args: str, timeout: float = 20.0) -> str:
    """Run a bluetoothctl command, return its output (empty on failure)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "bluetoothctl", *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return out.decode(errors="ignore")
    except Exception as err:  # noqa: BLE001 - bluetoothctl missing/timeout
        log.debug("bluetoothctl %s: %s", args, err)
        return ""


async def _bctl_pair(address: str) -> str:
    """Run an interactive bluetoothctl session that registers an agent and
    pairs (Just Works needs an agent to auto-confirm). Returns the output."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "bluetoothctl",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except Exception as err:  # noqa: BLE001
        return f"bluetoothctl unavailable: {err}"
    seq = [
        ("power on", 1.0),
        ("agent KeyboardDisplay", 0.3),
        ("default-agent", 0.3),
        ("scan on", 6.0),
        (f"pair {address}", 12.0),
        (f"trust {address}", 1.0),
        ("scan off", 0.3),
        ("quit", 0.3),
    ]
    try:
        for cmd, delay in seq:
            proc.stdin.write((cmd + "\n").encode())
            await proc.stdin.drain()
            await asyncio.sleep(delay)
    except Exception:  # noqa: BLE001
        pass
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        return out.decode(errors="ignore")
    except asyncio.TimeoutError:
        proc.kill()
        return "(bluetoothctl session timed out)"


async def ensure_bonded(address: str) -> bool:
    """Make sure BlueZ has a trusted bond for the bike (Just Works pairing).

    Runs whenever the bike is detected, so pairing happens at the right moment
    (bike awake / in pairing mode) without restarting — no missed window.
    """
    if "Bonded: yes" in await _bctl("info", address, timeout=10):
        # Already bonded: just (re)trust and connect immediately. Do NOT
        # disconnect here — that only added latency and caused org.bluez
        # "In Progress" races, and with the bike on only briefly the read
        # window would close before we got to connect.
        await _bctl("trust", address, timeout=8)
        return True
    log.warning("not bonded — pairing %s now (bike must be in PAIRING MODE)", address)
    publish_status("Not paired — put the bike in PAIRING MODE", "ON")
    out = await _bctl_pair(address)
    tail = " | ".join(ln.strip() for ln in out.splitlines()
                      if any(k in ln for k in ("Pair", "pair", "Fail", "Agent", "Bonded", "Error")))
    log.info("pair log: %s", tail[-400:] or "(no relevant output)")
    bonded = "Bonded: yes" in await _bctl("info", address, timeout=10)
    log.info("pairing attempt result: bonded=%s", bonded)
    if not bonded:
        return False
    # The pairing session holds the link open; free the single BLE slot so the
    # follow-up read gets a clean connection (and the bike advertises again).
    await _bctl("disconnect", address, timeout=8)
    await asyncio.sleep(1)
    return True


# -------------------------------------------------------------------- BLE
def _pb_strings(b: bytes) -> list[str]:
    """Extract protobuf string fields (0a <len> <ascii>) from a push frame —
    the bike's component-info records (model, battery, serials, …)."""
    out: list[str] = []
    i = 0
    while i < len(b) - 1:
        if b[i] == 0x0A:
            ln = b[i + 1]
            if 3 <= ln <= 40 and i + 2 + ln <= len(b):
                chunk = b[i + 2:i + 2 + ln]
                if all(32 <= c < 127 for c in chunk):
                    out.append(chunk.decode())
                    i += 2 + ln
                    continue
        i += 1
    return out


async def read_push(client: BleakClient) -> tuple[str | None, dict[str, int] | None]:
    """Subscribe to the Bosch push channel and capture the live ride mode and
    the estimated range per mode.

    Best-effort: enables notifications, replays the app's stream subscriptions
    (so the bike pushes the mode (9809) and range (9857) attributes), listens
    briefly, and returns (mode, ranges). Either may be None if nothing arrived.
    """
    latest: dict[str, object] = {"mode": None, "range": None, "model": None,
                                 "battery_model": None, "drive_unit": None,
                                 "display": None}
    count = {"n": 0}
    buf = bytearray()  # full stream, for cross-frame component records

    def cb(_char, data: bytearray) -> None:
        count["n"] += 1
        b = bytes(data)
        buf.extend(b)
        log.debug("push frame %d: %s", count["n"], b.hex())
        m = parse_mode(b)
        if m:
            latest["mode"] = m
        r = parse_range(b)
        if r:
            latest["range"] = r
        for svalue in _pb_strings(b):  # component info (model, battery, …)
            if svalue == "Urban Arrow":
                latest["model"] = svalue
            elif svalue.startswith("PowerPack"):
                latest["battery_model"] = svalue.replace(" Frame", "").strip()
            elif "Performance Line" in svalue or "Drive Unit" in svalue:
                latest["drive_unit"] = svalue.replace("Drive Unit", "").strip() or svalue
            elif svalue.startswith("Kiox") or svalue.startswith("Nyon") or "Purion" in svalue:
                latest["display"] = svalue

    try:
        for attempt in range(2):  # tolerate a "service discovery not done yet" race
            try:
                await client.start_notify(PUSH_NOTIFY, cb)
                break
            except Exception as err:  # noqa: BLE001
                if attempt == 0:
                    log.debug("start_notify retry after: %s", err)
                    await asyncio.sleep(1.5)
                else:
                    raise
        log.debug("subscribed to push channel %s; sending subscriptions", PUSH_NOTIFY)
        # Replayed verbatim from the Bosch app (Flow.pklg): the registration
        # header, then the ride-mode (9809) and per-mode range (9857) attribute
        # subscriptions. The bike then pushes 30 04 98 09 08 <level> and
        # 98 57 0a 04 <eco><tour><auto><turbo>.
        for cmd in (
            "1002010310030400f410020301100203021002030310020304100203051002030610020307",
            "30054180980960",
            "30054180985760",
        ):
            try:
                await client.write_gatt_char(PUSH_WRITE, bytes.fromhex(cmd), response=False)
            except Exception as err:  # noqa: BLE001
                log.debug("sub write failed: %s", err)
        await asyncio.sleep(6)
        await client.stop_notify(PUSH_NOTIFY)
        log.debug("push channel: %d frame(s), mode=%s range=%s",
                  count["n"], latest["mode"], latest["range"])
        # NB: bike_model is config-driven (_model_name), not taken from the BLE
        # component string (which only ever yields the generic "Urban Arrow").
        for src, dst in (("battery_model", "battery_model"),
                         ("drive_unit", "drive_unit"), ("display", "display")):
            if latest[src]:
                _last[dst] = latest[src]
        comps = parse_components(bytes(buf))  # per-subsystem firmware + date
        if comps:
            _last["components"] = {**_last.get("components", {}), **comps}
        if latest["model"] or latest["battery_model"] or comps:
            log.info("components: battery=%s drive=%s display=%s specs=%s",
                     latest["battery_model"], latest["drive_unit"],
                     latest["display"], comps)
            _save_cfg()  # persist so the specs show immediately after a restart
    except Exception as err:  # noqa: BLE001
        log.warning("push read failed: %s: %s", type(err).__name__, err)
    return latest["mode"], latest["range"]  # type: ignore[return-value]


async def read_snapshot(mqtt_client: mqtt.Client, device) -> bool:
    """Connect, read eb21 battery + push-channel ride mode, publish (retained)."""
    log.info("connecting to %s (%s) ...", device.address, device.name or "?")
    publish_status(f"Connected to {device.name or 'bike'} — reading…", "ON")
    raw: bytes | None = None
    mode: str | None = None
    ranges: dict[str, int] | None = None
    async with BleakClient(device, timeout=20.0) as client:
        log.info("reading eb21 snapshot ...")
        for attempt in range(3):
            try:
                raw = bytes(await asyncio.wait_for(client.read_gatt_char(EB21), timeout=10))
                break
            except Exception as err:  # noqa: BLE001
                log.warning("read attempt %d -> %s: %s", attempt + 1, type(err).__name__, err)
                await asyncio.sleep(2)
        mode, ranges = await read_push(client)
        if "device_info" not in _last:  # static — read once
            info = {}
            for key, uuid in DEVICE_INFO_CHARS.items():
                try:
                    val = bytes(await client.read_gatt_char(uuid)).decode(errors="ignore").strip()
                    if val:
                        info[key] = val
                except Exception:  # noqa: BLE001
                    pass
            if info:
                info["name"] = device.name or ""
                _last["device_info"] = info
                # Promote to top-level so they ride along in the STATE publish
                # (and become HA sensors). "serial" (DIS 2a25) is Bosch's part
                # number; the bike's frame number comes from eb41 instead.
                if info.get("firmware"):
                    _last["hub_firmware"] = info["firmware"]
                if info.get("serial"):
                    _last["part_number"] = info["serial"]
                if info.get("model"):
                    _last["model_number"] = info["model"]
                log.info("device info: %s", info)
        if "frame_number" not in _last:  # static — read once from eb41
            try:
                fr = parse_eb41_frame(bytes(await client.read_gatt_char(EB41)))
                if fr:
                    _last["frame_number"] = fr
                    log.info("frame number: %s", fr)
            except Exception as err:  # noqa: BLE001
                log.debug("eb41 read failed: %s", err)
    if raw is None:
        log.warning("eb21 read failed (bond missing/untrusted? re-pair via bluetoothctl)")
        publish_status("Read failed — bike awake?", "ON")
        return False
    log.debug("eb21 raw: %s", raw.hex())
    data = parse_eb21(raw)
    battery = data.get("battery")
    if battery is None:
        log.warning("no battery field in payload: %s", raw.hex())
        publish_status("Read failed — no battery field", "ON")
        return False
    now = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    state = {
        "battery": battery,
        "last_updated": now,
        "address": device.address,
    }
    if "odometer" in data:
        state["odometer"] = data["odometer"]
    if "next_service" in data:
        state["next_service"] = data["next_service"]
    for k in ("frame_number", "hub_firmware", "part_number",
              "model_number", "module_firmware"):
        if _last.get(k):
            state[k] = _last[k]
    mqtt_client.publish(STATE_TOPIC, json.dumps(state), retain=True)
    log.info("published %s", state)
    _last.update(state)
    _last["bonded"] = True
    if mode is not None:
        mqtt_client.publish(MODE_TOPIC, json.dumps({"mode": mode}), retain=True)
        log.info("ride mode: %s", mode)
        _last["mode"] = mode
    if ranges is not None:
        mqtt_client.publish(RANGE_TOPIC, json.dumps(ranges), retain=True)
        log.info("range km: %s", ranges)
        _last["range"] = ranges
    publish_status(f"Battery {battery}% read at {time.strftime('%Y-%m-%d %H:%M')}", "ON")
    return True


async def write_lock(device, on: bool) -> bool:
    """Apply the eBike Lock setting on the (awake) bike, then trigger 'apply'.
    ON  = attribute 8d1c = 0x40 (captured: lock on, phone unlock).
    OFF = attribute 8d1c = 0x00 (inferred: clears the enable bit — the app omits
          8d1c entirely when off, so no explicit off value exists in the captures).
    Each is one attribute-addressed record (does not touch other settings),
    followed by the 8d2f apply-settings trigger. All writes go to PUSH_WRITE."""
    global _lock_seq
    val = "40" if on else "00"
    _lock_seq = (_lock_seq + 1) & 0xFF
    seq = f"{_lock_seq:02x}"
    async with BleakClient(device, timeout=20.0) as client:
        # Registration header the app/read_push sends before settings traffic.
        await client.write_gatt_char(PUSH_WRITE,
                                     bytes.fromhex("1002010310030400f4"), response=False)
        await client.write_gatt_char(PUSH_WRITE,
                                     bytes.fromhex("300540808d1c" + val), response=False)
        await client.write_gatt_char(PUSH_WRITE,
                                     bytes.fromhex("300840808d2f" + seq + "0a0101"),
                                     response=False)
    return True


def _tracker_module_mac(adv) -> "str | None":
    """The COMODULE's fixed module MAC from its advertisement (company 0x020F)."""
    raw = (adv.manufacturer_data or {}).get(0x020F)
    if raw and len(raw) >= 6:
        return ":".join(f"{b:02x}" for b in raw[:6]).upper()
    return None


def _record(device, adv) -> "str | None":
    """Record a discovered bike/tracker for the setup UI. Returns its kind."""
    nl = (device.name or "").lower()
    if NAME_MATCH in nl:
        kind = "bike"
        mac = None
    elif COMODULE_NAME in nl:
        kind = "tracker"
        mac = _tracker_module_mac(adv)
    else:
        return None
    _discovered[device.address] = {
        "address": device.address, "name": device.name or "",
        "rssi": adv.rssi, "kind": kind, "module_mac": mac, "ts": time.time(),
    }
    return kind


async def find_bike(timeout: float = 15.0):
    """Scan with a FRESH scanner (started+stopped per call, so it can't wedge
    after a connect) and return the bike's BLEDevice, or None."""
    found: dict[str, object] = {}
    ev = asyncio.Event()
    seen: set[str] = set()

    def cb(device, adv) -> None:
        kind = _record(device, adv)
        if kind != "bike":
            return
        if _bike_addr is None:  # auto-detect by name
            if _locked_addr and device.address.upper() != _locked_addr.upper():
                return  # locked onto our bike — ignore other "smart system eBike"s
            if device.address not in seen:
                seen.add(device.address)
                log.info("bike candidate: %s  '%s'  rssi=%s",
                         device.address, device.name or "", adv.rssi)
        elif device.address.upper() != _bike_addr.upper():
            return
        found["device"] = device
        ev.set()

    async with _scan_lock, BleakScanner(detection_callback=cb):
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        except asyncio.TimeoutError:
            pass
    return found.get("device")


async def ble_loop(mqtt_client: mqtt.Client) -> None:
    """Scan (fresh each cycle); on detection, bond if needed and read once."""
    global _locked_addr, _lock_cmd
    last_ok = 0.0
    log.info("scanning (%s)", _bike_addr or "auto-detect 'smart system eBike'")
    while True:
        try:
            device = await find_bike(timeout=15.0)
            auto = _bike_addr is None
            if device is not None and _lock_cmd is not None:
                # A lock toggle is pending — apply it now (bike is awake/in range).
                want_on = _lock_cmd == "ON"
                try:
                    if await ensure_bonded(device.address) and await write_lock(device, want_on):
                        publish_lock("ON" if want_on else "OFF")
                        log.info("eBike Lock set to %s", "ON" if want_on else "OFF")
                    else:
                        log.warning("eBike Lock write failed (not bonded?)")
                except Exception as err:  # noqa: BLE001
                    log.warning("lock write error: %s: %s", type(err).__name__, err)
                _lock_cmd = None
            if device is None:
                publish_status("Bike not found (off or out of range)", "OFF")
            elif time.time() - last_ok < COOLDOWN:
                # Seen recently; wait out the cooldown before reading again.
                publish_status("Bike in range — waiting (cooldown)", "ON")
            elif (auto and _locked_addr is None and device.address in _pair_fail
                  and time.time() - _pair_fail[device.address] < PAIR_RETRY_AFTER):
                # A nearby eBike we couldn't pair with (a neighbour's) — skip it.
                publish_status("Ignoring an unknown nearby eBike", "OFF")
            else:
                log.info("bike seen — connecting to read")
                if not await ensure_bonded(device.address):
                    _pair_fail[device.address] = time.time()
                elif await read_snapshot(mqtt_client, device):
                    last_ok = time.time()
                    if auto and _locked_addr is None:
                        _locked_addr = device.address
                        _pair_fail.clear()
                        log.info("locked onto bike %s", device.address)
                    # Bike is on -> refresh the tracker's own battery too (low-power).
                    await read_tracker_battery()
        except Exception as err:  # noqa: BLE001
            log.warning("cycle failed: %s: %s", type(err).__name__, err or "(timeout)")
            publish_status("Connection failed — keep the bike on, retrying…", "ON")
        await asyncio.sleep(SCAN_GAP)


# -------------------------------------------------------------------- COMODULE
async def find_comodule(timeout: float = 12.0):
    """Scan for the URBANARROW tracker. If a module MAC is configured, match it
    (robust against the rotating BLE address); else take the first one. Records
    all trackers for the setup UI. Returns the BLEDevice or None."""
    found: dict[str, object] = {}
    seen: dict[str, int] = {}
    ev = asyncio.Event()

    def cb(device, adv) -> None:
        if _record(device, adv) != "tracker":
            return
        seen[device.address] = getattr(adv, "rssi", 0)
        if _tracker_mac and (_tracker_module_mac(adv) or "").upper() != _tracker_mac.upper():
            return  # not our tracker
        # Record the stable module MAC (adv) + current BLE address for the UI.
        _last["module_mac"] = _tracker_module_mac(adv) or _last.get("module_mac")
        _last["tracker_addr"] = device.address
        found["device"] = device
        ev.set()

    log.info("comodule scan: looking for tracker (filter=%s)", _tracker_mac or "any")
    async with _scan_lock, BleakScanner(detection_callback=cb):
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        except asyncio.TimeoutError:
            pass
    if seen:
        log.info("comodule scan: trackers seen: %s",
                 ", ".join(f"{a}@{r}dBm" for a, r in seen.items()))
    else:
        log.info("comodule scan: no URBANARROW tracker heard by the local adapter")
    return found.get("device")


async def motion_watcher() -> None:
    """Keep a connection to the tracker and watch 155e for motion.

    The tracker streams 0xD1 frames in a burst while the bike is physically moved
    (even with the eBike off). Publish motion ON on the first 0xD1 and OFF after
    MOTION_OFF_DELAY seconds of stillness. Re-resolves the tracker each reconnect
    so a change made in the setup UI takes effect, and honours the disabled flag.
    """
    state = {"on": False, "last": 0.0, "since": 0.0}

    def cb(_c, data: bytearray) -> None:
        b = bytes(data)
        if len(b) > 2 and b[1] == 0xC6:        # COMODULE status: byte2 = its own battery %
            bat = b[2]
            if 0 <= bat <= 100 and _last.get("tracker_battery") != bat:
                _last["tracker_battery"] = bat
                if _mqtt is not None:
                    _mqtt.publish(TRACKER_TOPIC, json.dumps({"battery": bat}), retain=True)
        if len(b) > 1 and b[1] == FRAME_MOTION:
            now = time.time()
            state["last"] = now
            if not state["on"]:
                state["on"] = True
                state["since"] = now
                publish_motion(True)
                _last["motion"] = True
                log.info("motion: ON")

    while True:
        if not _want_tracker():
            # Disarmed (or disabled): let the tracker sleep to save its battery.
            if state["on"]:
                state["on"] = False
                publish_motion(False)
                _last["motion"] = False
            _last["tracker_connected"] = False
            await asyncio.sleep(4)
            continue
        target = await find_comodule()
        if target is None:
            await asyncio.sleep(8)
            continue
        try:
            async with BleakClient(target, timeout=20.0) as client:
                await client.start_notify(CHAR_155E, cb)
                log.info("COMODULE motion watcher connected (%s)", target.address)
                _last["tracker_connected"] = True
                await _read_module_dis(client)  # static module specs, read once
                while client.is_connected and _want_tracker():
                    await asyncio.sleep(1)
                    now = time.time()
                    tb = _last.get("tracker_battery")
                    if (isinstance(tb, int) and tb <= 20
                            and _alarm["state"] in ARMED_STATES):
                        # Tracker's own battery is low — turn the alarm off so it
                        # can sleep/charge (a notification is sent by HA).
                        _alarm["state"] = "disarmed"
                        _alarm["fired"] = False
                        publish_alarm("disarmed")
                        publish_status(f"Tracker low ({tb}%) — charge it; alarm off", "ON")
                        log.info("tracker low (%s%%) — alarm auto-disabled", tb)
                        break
                    if state["on"] and now - state["last"] > MOTION_OFF_DELAY:
                        state["on"] = False
                        publish_motion(False)
                        _last["motion"] = False
                        _alarm["fired"] = False  # let the next movement trigger again
                        log.info("motion: OFF")
                    if (state["on"] and not _alarm_off and not _alarm["fired"]
                            and _alarm["state"] in ARMED_STATES
                            and now - state["since"] >= 3):
                        _alarm["fired"] = True
                        _alarm["state"] = "triggered"
                        publish_alarm("triggered")
                        log.info("alarm TRIGGERED by motion")
        except Exception as err:  # noqa: BLE001
            log.warning("motion watcher: %s: %s", type(err).__name__, err)
        _last["tracker_connected"] = False
        if state["on"]:
            state["on"] = False
            publish_motion(False)
            _last["motion"] = False
        await asyncio.sleep(5)  # brief backoff before reconnecting


async def _read_module_dis(client) -> None:
    """Read the COMODULE's Device Information once -> _last module_* fields."""
    mapping = {"module_firmware": "firmware", "module_manufacturer": "manufacturer",
               "module_hardware": "hardware", "module_model": "model"}
    if all(_last.get(dst) for dst in mapping):
        return
    for dst, key in mapping.items():
        if _last.get(dst):
            continue
        try:
            val = bytes(await client.read_gatt_char(
                DEVICE_INFO_CHARS[key])).decode(errors="ignore").strip()
            if val:
                _last[dst] = val
        except Exception:  # noqa: BLE001
            pass
    log.info("module info: fw=%s mfr=%s hw=%s", _last.get("module_firmware"),
             _last.get("module_manufacturer"), _last.get("module_hardware"))


async def read_tracker_battery() -> None:
    """One-shot, low-power refresh of the tracker's own battery %. Used while the
    bike is on (its main battery present, so the module is charging) and the alarm
    isn't already holding the connection. Briefly connects, grabs a 0xC6 status
    frame, then disconnects so the tracker can sleep again."""
    if _tracker_off or _want_tracker():
        return  # disabled, or the motion watcher already keeps it fresh
    target = await find_comodule()
    if target is None:
        return
    got = {"done": False}

    def cb(_c, data: bytearray) -> None:
        b = bytes(data)
        if len(b) > 2 and b[1] == 0xC6 and 0 <= b[2] <= 100:
            _last["tracker_battery"] = b[2]
            if _mqtt is not None:
                _mqtt.publish(TRACKER_TOPIC, json.dumps({"battery": b[2]}), retain=True)
            got["done"] = True

    try:
        async with BleakClient(target, timeout=20.0) as client:
            await client.start_notify(CHAR_155E, cb)
            await _read_module_dis(client)  # static module specs, read once
            for _ in range(8):
                await asyncio.sleep(1)
                if got["done"]:
                    break
            await client.stop_notify(CHAR_155E)
        if got["done"]:
            log.info("tracker battery refreshed: %s%%", _last.get("tracker_battery"))
    except Exception as err:  # noqa: BLE001
        log.debug("tracker battery refresh failed: %s", err)


async def start_motion(_mqtt_client: mqtt.Client) -> None:
    """Launch the self-resolving motion watcher (no-op work if disabled)."""
    publish_motion(False)
    asyncio.create_task(motion_watcher())


# ------------------------------------------------------------- setup UI (Ingress)
INDEX_HTML = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content='width=device-width,initial-scale=1'><title>Bosch Kiox eBike</title>
<style>
:root{--bg:#f2f3f5;--card:#fff;--soft:#eef1f4;--ink:#212121;--mut:#727272;--line:#e0e0e0;--acc:#03a9f4;--chip:#e9eaee}
@media(prefers-color-scheme:dark){:root{--bg:#111;--card:#1c1c1c;--soft:#262626;--ink:#e1e1e1;--mut:#9b9b9b;--line:#3a3a3a;--acc:#03a9f4;--chip:#2a2a2a}}
*{box-sizing:border-box}
body{font-family:-apple-system,system-ui,sans-serif;margin:0;background:var(--bg);color:var(--ink);line-height:1.45}
.wrap{max-width:none;margin:0 auto;padding:22px 22px 48px}
.tabs{display:flex;gap:8px;margin:2px 0 22px;max-width:520px}
.tab{flex:1;background:var(--chip);color:var(--mut);border:0;border-radius:12px;padding:12px;font-size:14px;font-weight:600;cursor:pointer}
.tab.on{background:var(--acc);color:#fff}
.dash{display:grid;gap:16px;grid-template-columns:1fr}
@media(min-width:720px){.dash{grid-template-columns:1.55fr 1fr;align-items:start}
 .col-wide{grid-column:1}.col-rail{grid-column:2;grid-row:1 / span 6}}
.rail{display:flex;flex-direction:column;gap:16px}
.card{background:var(--card);border-radius:12px;padding:20px;border:1px solid var(--line);box-shadow:0 1px 2px rgba(0,0,0,.04)}
.set .card{margin-bottom:18px}
.hero{text-align:center;padding:30px 24px 26px}
.htitle{font-size:21px;font-weight:800;letter-spacing:-.01em}
.badge{display:inline-block;font-size:11.5px;font-weight:700;letter-spacing:.06em;padding:6px 15px;border-radius:20px;background:var(--chip);color:var(--mut);margin-top:14px;text-transform:uppercase}
.badge.on{background:rgba(67,160,71,.18);color:#43a047}
.sub{color:var(--mut);font-size:12.5px;margin-top:9px}
.reqlink{display:inline-block;margin:14px 0 18px;font-size:12px;color:var(--mut);text-decoration:none;border:1px solid var(--line);padding:6px 13px;border-radius:18px}
.reqlink:hover{color:var(--acc);border-color:var(--acc)}
.bikewrap{background:#eef1f3;border-radius:12px;margin:24px 0 22px;padding:24px 18px}
.bike{display:block;margin:0 auto;width:100%;max-width:340px;height:auto}
.hstats{display:flex;justify-content:center;align-items:center;gap:32px;padding-top:4px}
.hstats .vr{width:1px;height:42px;background:var(--line)}
.segs{display:flex;gap:4px;align-items:flex-end;height:30px}
.segs i{width:8px;border-radius:3px;display:block}
.segs i:nth-child(1){height:13px}.segs i:nth-child(2){height:17px}.segs i:nth-child(3){height:21px}
.segs i:nth-child(4){height:26px}.segs i:nth-child(5){height:30px}
.pct{font-size:34px;font-weight:800;line-height:1}.pct small{font-size:17px;font-weight:600;color:var(--mut)}
.range{font-size:30px;font-weight:800}.range small{font-size:15px;color:var(--mut);font-weight:600}
.lbl{font-size:11px;letter-spacing:.08em;color:var(--mut);text-transform:uppercase;font-weight:700;margin-bottom:12px}
.big{font-size:28px;font-weight:800;line-height:1.1}
.g4{display:flex;justify-content:space-between;gap:12px;margin-top:4px}
.g4 div{flex:1}
.g4 .m{font-size:12px;font-weight:800;letter-spacing:.03em}.g4 .v{font-size:19px;font-weight:700;margin-top:7px}.g4 .v small{font-size:11px;color:var(--mut)}
.cbar{display:flex;height:10px;border-radius:6px;overflow:hidden;margin-top:20px;background:var(--line)}
.cbar i{display:block;height:100%}
.between{display:flex;justify-content:space-between;align-items:center}
.pill{font-size:13px;font-weight:700;padding:6px 14px;border-radius:20px}
button{background:var(--acc);color:#fff;border:0;border-radius:12px;padding:11px 16px;font-size:14px;cursor:pointer;margin:6px 8px 0 0}
button.sec{background:var(--chip);color:var(--ink)}button:disabled{opacity:.5;cursor:default}
.armbtns{margin-top:16px}.armbtns button{padding:10px 15px}
.warn{margin-top:12px;font-size:11.5px;color:var(--mut);line-height:1.45}
.tech{display:grid;grid-template-columns:auto 1fr;gap:9px 18px;font-size:13.5px;margin:0}
.tech dt{color:var(--mut)}.tech dd{margin:0;text-align:right;font-weight:600;word-break:break-all}
.th{font-size:11px;letter-spacing:.06em;color:var(--mut);text-transform:uppercase;font-weight:700;margin:16px 0 9px}
.th:first-child{margin-top:2px}
.legal{margin:26px 4px 8px;font-size:11px;line-height:1.5;color:var(--mut);text-align:center;opacity:.85}
.row{display:flex;align-items:center;gap:10px;padding:14px;border:1px solid var(--line);border-radius:12px;margin:8px 0;cursor:pointer}
.row.sel{border-color:var(--acc);background:rgba(3,169,244,.12)}
.muted{color:var(--mut);font-size:13px}.ok{color:#43a047;font-weight:700}.bad{color:#e53935;font-weight:700}
.hidden{display:none}h2{font-size:17px;margin:0 0 10px}.set p{margin:6px 0 14px}
</style></head><body><div class=wrap>
<div class=tabs>
  <button class='tab on' id=tabDash data-i18n=tab_dash onclick="tab('dash')">Dashboard</button>
  <button class=tab id=tabMore data-i18n=tab_more onclick="tab('more')">Meer info</button>
  <button class=tab id=tabSet data-i18n=tab_set onclick="tab('set')">Instellingen</button>
</div>

<section id=dash class=dash>
  <div class='card hero col-wide'>
    <div class=htitle id=bikeTitle>Urban Arrow</div>
    <div class=sub id=bikeSpec></div>
    <span class=badge id=conn>—</span>
    <div class=bikewrap>
      <img class=bike src="bike.png" alt="Urban Arrow Family" />
    </div>
    <a class=reqlink id=reqPhoto data-i18n=request_photo target=_blank rel=noopener
       href="https://github.com/bramboe/urban-arrow-ha/issues/new?title=Bike%20photo%20request&labels=bike-image&body=Model%20(Family%2FTender%2FCargo%2FFlatbed)%3A%20%0AType%2Fversion%20(e.g.%20Advanced%20Next)%3A%20%0AColour%3A%20%0AProduct%20photo%20URL%20(transparent%20PNG%20if%20possible)%3A%20">Andere fiets? Vraag je model aan</a>
    <div class=hstats>
      <div style='display:flex;align-items:center;gap:10px'><div class=segs id=segs></div><div class=pct id=pct>—<small>%</small></div></div>
      <div class=vr></div>
      <div class=range id=range>—<small> km</small></div>
    </div>
  </div>

  <div class='rail col-rail'>
    <div class=card><div class=lbl data-i18n=mode>Rijmodus</div><div class=between><div class=big id=mode>—</div><span class=pill id=modePill></span></div></div>
    <div class=card><div class=lbl data-i18n=maint>Onderhoud</div><div class=big id=service>—</div><div class=sub data-i18n=maint_sub>tot de volgende servicebeurt</div></div>
    <div class=card><div class=lbl data-i18n=security>Beveiliging</div><div id=secLine class=big>—</div>
      <div class=armbtns id=armBox></div><div class=warn id=secWarn></div>
      <div class=lbl data-i18n=elock style="margin:18px 0 8px">eBike Lock</div>
      <div id=lockState class=big style="font-size:20px">—</div>
      <div class=armbtns id=lockBox></div><div class=warn id=lockWarn></div></div>
  </div>

  <div class='card col-wide'><div class=lbl data-i18n=ranges>Geschat bereik per stand</div>
    <div class=g4 id=ranges></div><div class=cbar id=rangeBar></div></div>

  <div class='card col-wide'><div class=lbl data-i18n=mileage>Kilometerstand</div><div class=big id=odo>—</div></div>
</section>

<section id=more class='set hidden'>
  <div class=card><div class=lbl data-i18n=tech>Technische info</div>
    <div id=techInfo></div></div>
  <div class=card><div class=lbl data-i18n=components>Componenten</div>
    <div id=compInfo></div></div>
  <div class=card><div class=lbl data-i18n=about>Over deze add-on</div>
    <p class=muted data-i18n=about_p></p>
    <p><a id=repoLink href="https://github.com/bramboe/urban-arrow-ha" target=_blank rel=noopener data-i18n=about_repo>Broncode op GitHub</a></p>
    <div class=legal data-i18n=legal></div></div>
</section>

<section id=set class='set hidden'>
  <div class=card>
    <h2 data-i18n=su_bike_h>1. Fiets</h2><p class=muted data-i18n=su_bike_p>Zet het display van de fiets aan en scan.</p>
    <button data-i18n=scan_bikes onclick="scan('bike')">Scan fietsen</button><div id=bikes></div>
    <div id=bikeActions class=hidden><button data-i18n=select_bike onclick="selectBike()">Selecteer deze fiets</button></div>
    <div id=pairBox class=hidden style='margin-top:8px'>
      <p class=muted data-i18n=su_pair_p>Zet de fiets in pairing mode (display → nieuw apparaat koppelen), klik dan:</p>
      <button id=pairBtn data-i18n=pair_btn onclick="pair()">Koppel (pair)</button><span id=pairMsg></span></div>
  </div>
  <div class=card>
    <h2 data-i18n=su_tracker_h>2. GPS-tracker (anti-diefstal, optioneel)</h2>
    <p class=muted data-i18n=su_tracker_p>De tracker is altijd aan. Scan en kies 'm, of sla over.</p>
    <button data-i18n=scan_trackers onclick="scan('tracker')">Scan trackers</button>
    <button class=sec data-i18n=skip onclick="skipTracker()">Overslaan / uit</button><div id=trackers></div>
    <div id=trackerActions class=hidden><button data-i18n=select_tracker onclick="selectTracker()">Selecteer deze tracker</button></div>
  </div>
  <div class=card>
    <h2 data-i18n=su_alarm_h>3. Alarm (optioneel — vereist de tracker)</h2>
    <p class=muted data-i18n=su_alarm_p>Afwezig = hard (push + lampen), Thuis = stil (alleen melding). Uit = alleen de bewegingssensor.</p>
    <button id=alarmBtn onclick="toggleAlarm()">…</button> <span id=alarmState class=muted></span>
  </div>
</section>
</div>
<script>
const $=s=>document.querySelector(s);
const api=async(p,o)=>(await fetch(p,o)).json();
const post=(p,b)=>api(p,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b||{})});
const LANG=(navigator.language||'en').toLowerCase().startsWith('nl')?'nl':'en';
const T={nl:{tab_dash:'Dashboard',tab_more:'Meer info',tab_set:'Instellingen',components:'Componenten',about:'Over deze add-on',about_p:'Leest je Bosch Smart System eBike (Kiox) via Bluetooth uit en publiceert batterij, bereik, modus, km-stand, beurt, beweging/alarm en tracker naar Home Assistant.',about_repo:'Broncode op GitHub',c_drive:'Aandrijving',c_batt:'Accu',c_disp:'Display',c_hub:'Hub',mode:'Rijmodus',maint:'Onderhoud',maint_sub:'tot de volgende servicebeurt',security:'Beveiliging',ranges:'Geschat bereik per stand',mileage:'Kilometerstand',tech:'Technische info',tech_kiox:'Kiox (Bosch-hub)',tech_gps:'GPS-module',t_model:'Model',t_frame:'Framenummer',t_part:'Onderdeelnummer',t_hubfw:'Firmware',t_modfw:'Firmware',t_addr:'Bluetooth-adres',t_mac:'MAC-adres',t_mfr:'Fabrikant',t_hw:'Hardware',t_batt:'Module-accu',legal:'Onofficiële, door de community gemaakte integratie. Niet gelieerd aan, goedgekeurd of ondersteund door Bosch eBike Systems of Urban Arrow. Gebruik volledig op eigen risico, zonder enige garantie. Alle merknamen zijn eigendom van hun respectievelijke eigenaren.',su_bike_h:'1. Fiets',su_bike_p:"Zet het display van de fiets aan en scan.",scan_bikes:'Scan fietsen',select_bike:'Selecteer deze fiets',su_pair_p:'Zet de fiets in pairing mode (display → nieuw apparaat koppelen), klik dan:',pair_btn:'Koppel (pair)',su_tracker_h:'2. GPS-tracker (anti-diefstal, optioneel)',su_tracker_p:"De tracker is altijd aan. Scan en kies 'm, of sla over.",scan_trackers:'Scan trackers',skip:'Overslaan / uit',select_tracker:'Selecteer deze tracker',su_alarm_h:'3. Alarm (optioneel — vereist de tracker)',su_alarm_p:'Afwezig = hard (push + lampen), Thuis = stil (alleen melding). Uit = alleen de bewegingssensor.',conn_on:'Verbonden',conn_off:'Niet verbonden',no_reading:'nog geen meting',up_now:'zojuist bijgewerkt',up_min:'bijgewerkt {n} min geleden',up_hour:'bijgewerkt {n} uur geleden',up_day:'bijgewerkt {n} d geleden',motion_y:'beweging',motion_n:'rustig',alarm_off:'Alarm uit',s_disarmed:'Uit',s_home:'Stil',s_away:'Vol alarm',s_trig:'⚠️ GEACTIVEERD',a_off:'Uit',a_home:'Stil',a_away:'Vol alarm',alarm_off_hint:'Alarm staat uit (zie Instellingen)',alarm_enable:'Alarm inschakelen',alarm_disable:'Alarm uitschakelen',now_off:'momenteel uit',now_on:'momenteel aan',scanning:'scannen… (±8s)',nothing:'niets gevonden — staat het apparaat aan/in bereik?',pairing:'koppelen…',paired_ok:'Gekoppeld ✓',paired_fail:'Mislukt — staat de fiets in pairing mode?',request_photo:'Andere fiets? Vraag je kleur/model aan',sec_warn:'⚠️ Let op: scherp zetten houdt de tracker verbonden — daardoor loopt de module-accu sneller leeg. Bij ≤20% schakelt het alarm automatisch uit.',elock:'eBike Lock',lk_locked:'🔒 Vergrendeld',lk_unlocked:'🔓 Ontgrendeld',lk_unknown:'onbekend',lk_pending:'bezig…',btn_lock:'Vergrendel',btn_unlock:'Ontgrendel',lock_warn:'De fiets moet wakker/in bereik zijn; de actie wordt toegepast bij de eerstvolgende verbinding.'},
en:{tab_dash:'Dashboard',tab_more:'More info',tab_set:'Settings',components:'Components',about:'About this add-on',about_p:'Reads your Bosch Smart System eBike (Kiox) over Bluetooth and publishes battery, range, mode, odometer, service, motion/alarm and tracker to Home Assistant.',about_repo:'Source code on GitHub',c_drive:'Drive unit',c_batt:'Battery',c_disp:'Display',c_hub:'Hub',mode:'Ride mode',maint:'Maintenance',maint_sub:'until the next service',security:'Security',ranges:'Estimated range per mode',mileage:'Odometer',tech:'Technical info',tech_kiox:'Kiox (Bosch hub)',tech_gps:'GPS module',t_model:'Model',t_frame:'Frame number',t_part:'Part number',t_hubfw:'Firmware',t_modfw:'Firmware',t_addr:'Bluetooth address',t_mac:'MAC address',t_mfr:'Manufacturer',t_hw:'Hardware',t_batt:'Module battery',legal:'Unofficial, community-made integration. Not affiliated with, endorsed by, or supported by Bosch eBike Systems or Urban Arrow. Use entirely at your own risk, without any warranty. All trademarks are the property of their respective owners.',su_bike_h:'1. Bike',su_bike_p:"Turn on the bike's display and scan.",scan_bikes:'Scan bikes',select_bike:'Select this bike',su_pair_p:'Put the bike in pairing mode (display → connect a new device), then:',pair_btn:'Pair',su_tracker_h:'2. GPS tracker (anti-theft, optional)',su_tracker_p:'The tracker is always on. Scan and pick it, or skip.',scan_trackers:'Scan trackers',skip:'Skip / off',select_tracker:'Select this tracker',su_alarm_h:'3. Alarm (optional — needs the tracker)',su_alarm_p:'Away = loud (push + lights), Home = silent (notification only). Off = motion sensor only.',conn_on:'Connected',conn_off:'Not connected',no_reading:'no reading yet',up_now:'updated just now',up_min:'updated {n} min ago',up_hour:'updated {n} h ago',up_day:'updated {n} d ago',motion_y:'motion',motion_n:'still',alarm_off:'Alarm off',s_disarmed:'Off',s_home:'Silent',s_away:'Full alarm',s_trig:'⚠️ TRIGGERED',a_off:'Off',a_home:'Silent',a_away:'Full alarm',alarm_off_hint:'Alarm is off (see Settings)',alarm_enable:'Enable alarm',alarm_disable:'Disable alarm',now_off:'currently off',now_on:'currently on',scanning:'scanning… (±8s)',nothing:'nothing found — is the device on / in range?',pairing:'pairing…',paired_ok:'Paired ✓',paired_fail:'Failed — is the bike in pairing mode?',request_photo:'Different bike? Request your colour & model',sec_warn:'⚠️ Note: arming keeps the tracker connected — this drains the module battery faster. At ≤20% the alarm switches off automatically.',elock:'eBike Lock',lk_locked:'🔒 Locked',lk_unlocked:'🔓 Unlocked',lk_unknown:'unknown',lk_pending:'working…',btn_lock:'Lock',btn_unlock:'Unlock',lock_warn:'The bike must be awake/in range; the action is applied on the next connection.'}};
const t=(k,n)=>((T[LANG]||T.en)[k]||k).replace('{n}',n);
function applyI18n(){document.querySelectorAll('[data-i18n]').forEach(e=>{e.textContent=t(e.dataset.i18n)});}
const MC={Turbo:'#e2241a',Auto:'#7b3ff2','Tour+':'#1aa3e0',Tour:'#1aa3e0',Eco:'#5fb336',Off:'#8a8a8a'};
const bcol=p=>p>40?'#37a24a':p>15?'#f59e0b':'#e53935';
let pick={bike:null,tracker:null};
function tab(t){['dash','more','set'].forEach(x=>$('#'+x).classList.toggle('hidden',x!=t));
  $('#tabDash').classList.toggle('on',t=='dash');$('#tabMore').classList.toggle('on',t=='more');$('#tabSet').classList.toggle('on',t=='set');}
function ago(iso){if(!iso)return '';const ts=Date.parse(iso);if(isNaN(ts))return '';
  const s=Math.max(0,(Date.now()-ts)/1000);
  if(s<90)return t('up_now');if(s<3600)return t('up_min',Math.round(s/60));
  if(s<86400)return t('up_hour',Math.round(s/3600));return t('up_day',Math.round(s/86400));}
const fresh=iso=>{const ts=Date.parse(iso);return !isNaN(ts)&&(Date.now()-ts)<150000;};
async function refresh(){const s=await api('api/status');const L=s.last||{};const di=L.device_info||{};const dev=s.device||{};const R=L.range||{};
  $('#bikeTitle').textContent=L.bike_model||'Urban Arrow Connected';
  $('#bikeSpec').textContent=L.last_updated?ago(L.last_updated):t('no_reading');
  const f=fresh(L.last_updated);
  $('#conn').className='badge'+(f?' on':'');$('#conn').textContent=f?t('conn_on'):t('conn_off');
  const p=L.battery; const fill=p==null?0:Math.max(0,Math.min(5,Math.round(p/20)));
  let seg='';for(let i=0;i<5;i++)seg+=`<i style="background:${i<fill?bcol(p):'#dfe2e7'}"></i>`;$('#segs').innerHTML=seg;
  $('#pct').innerHTML=(p??'—')+'<small>%</small>';
  $('#range').innerHTML=(R.turbo!=null?`${R.turbo}–${R.eco}`:'—')+'<small> km</small>';
  $('#mode').textContent=L.mode||'—';const mp=$('#modePill');
  if(L.mode){mp.style.display='';mp.style.background=(MC[L.mode]||'#888')+'22';mp.style.color=MC[L.mode]||'#888';mp.textContent=L.mode;}else mp.style.display='none';
  const order=[['turbo','TURBO'],['auto','AUTO'],['tour','TOUR+'],['eco','ECO']];
  $('#ranges').innerHTML=order.map(([k,n])=>`<div><div class=m style="color:${MC[n=='TOUR+'?'Tour+':n[0]+n.slice(1).toLowerCase()]||'#555'}">${n}</div><div class=v>${R[k]??'—'}<small> km</small></div></div>`).join('');
  const rsum=order.reduce((a,[k])=>a+(R[k]||0),0)||1;
  $('#rangeBar').innerHTML=order.map(([k,n])=>`<i style="width:${(R[k]||0)/rsum*100}%;background:${MC[n=='TOUR+'?'Tour+':n[0]+n.slice(1).toLowerCase()]||'#999'}"></i>`).join('');
  $('#service').textContent=L.next_service!=null?L.next_service+' km':'—';
  $('#odo').textContent=L.odometer!=null?L.odometer.toLocaleString('nl-NL')+' km':'—';
  // technical info — Kiox (Bosch hub) vs GPS module, separate devices/MACs
  const dl=rows=>{const r=rows.filter(([k,v])=>v!=null&&v!=='');return r.length
    ?'<dl class=tech>'+r.map(([k,v])=>`<dt>${t(k)}</dt><dd>${v}</dd>`).join('')+'</dl>'
    :`<div class=muted style="margin:2px 0 6px">${t('no_reading')}</div>`;};
  const kiox=[['t_model',di.model||L.model_number],['t_frame',L.frame_number],['t_part',L.part_number||di.serial],['t_hubfw',L.hub_firmware||di.firmware],['t_addr',L.address||s.bike]];
  const gps=[['t_mac',s.tracker||L.module_mac],['t_modfw',L.module_firmware],['t_hw',L.module_hardware],['t_mfr',L.module_manufacturer],['t_batt',L.tracker_battery!=null?L.tracker_battery+'%':null]];
  $('#techInfo').innerHTML=`<div class=th>${t('tech_kiox')}</div>`+dl(kiox)+`<div class=th>${t('tech_gps')}</div>`+dl(gps);
  // components — name + firmware + production date per subsystem
  const C=L.components||{};
  const cv=(name,key)=>{const c=C[key]||{};const x=[name,c.firmware&&('fw '+c.firmware),c.date].filter(Boolean);return x.length?x.join(' · '):null;};
  const comp=[['c_drive',cv(L.drive_unit,'drive')],['c_batt',cv(L.battery_model,'battery')],['c_disp',cv(L.display,'display')],['c_hub',cv(di.model||L.model_number,'controller')]];
  $('#compInfo').innerHTML=dl(comp);
  // security
  const A=L.alarm; const nm={disarmed:t('s_disarmed'),armed_home:t('s_home'),armed_away:t('s_away'),triggered:t('s_trig')}[A]||'—';
  const mv=L.motion?t('motion_y'):t('motion_n');const tb=L.tracker_battery;
  $('#secLine').innerHTML=(s.alarm_off?t('alarm_off')+' · '+mv:`${nm} · ${mv}`)+(tb!=null?` · module ${tb}%`:'');
  $('#secWarn').textContent=t('sec_warn');
  $('#armBox').innerHTML=s.alarm_off?`<span class=muted>${t('alarm_off_hint')}</span>`:
    [['DISARM','a_off','disarmed'],['ARM_HOME','a_home','armed_home'],['ARM_AWAY','a_away','armed_away']].map(([c,lk,st])=>
     `<button class="${A==st?'':'sec'}" onclick="arm('${c}')">${t(lk)}</button>`).join('');
  // eBike Lock
  const lk=L.lock; const lkmap={ON:t('lk_locked'),OFF:t('lk_unlocked')};
  $('#lockState').textContent=lk?lkmap[lk]:t('lk_unknown');
  $('#lockBox').innerHTML=
    `<button class="${lk=='ON'?'':'sec'}" onclick="lock(true)">${t('btn_lock')}</button>`+
    `<button class="${lk=='OFF'?'':'sec'}" onclick="lock(false)">${t('btn_unlock')}</button>`;
  $('#lockWarn').textContent=t('lock_warn');
  // settings tab bits
  window._alarmOff=s.alarm_off;
  $('#alarmBtn').textContent=t(s.alarm_off?'alarm_enable':'alarm_disable');
  $('#alarmState').textContent=t(s.alarm_off?'now_off':'now_on');}
async function arm(cmd){await post('api/alarm',{cmd});refresh()}
async function lock(on){await post('api/lock',{on});$('#lockState').textContent=t('lk_pending');refresh()}
async function toggleAlarm(){await post('api/set_alarm',{on:window._alarmOff===true});refresh()}
const fmt=d=>`${d.address} · ${d.rssi} dBm`+(d.module_mac?` · ${d.module_mac}`:'');
async function scan(kind){const box=kind=='bike'?'#bikes':'#trackers';
  $(box).innerHTML=`<span class=muted>${t('scanning')}</span>`;
  const list=await api('api/scan',{method:'POST'});const items=list.filter(d=>d.kind==kind);
  if(!items.length){$(box).innerHTML=`<span class=muted>${t('nothing')}</span>`;return}
  $(box).innerHTML='';items.forEach(d=>{const el=document.createElement('div');el.className='row';
   el.innerHTML=`<div><b>${d.name||kind}</b><div class=muted style="font-size:12px">${fmt(d)}</div></div>`;
   el.onclick=()=>{pick[kind]=d;[...$(box).children].forEach(c=>c.classList.remove('sel'));el.classList.add('sel');
    $(kind=='bike'?'#bikeActions':'#trackerActions').classList.remove('hidden')};
   $(box).appendChild(el);});}
async function selectBike(){await post('api/select_bike',{address:pick.bike.address});$('#pairBox').classList.remove('hidden');refresh()}
async function pair(){$('#pairBtn').disabled=true;$('#pairMsg').textContent=' '+t('pairing');
  const r=await post('api/pair');$('#pairBtn').disabled=false;
  $('#pairMsg').innerHTML=r.ok?` <span class=ok>${t('paired_ok')}</span>`:` <span class=bad>${t('paired_fail')}</span>`;refresh()}
async function selectTracker(){await post('api/select_tracker',{module_mac:pick.tracker.module_mac});refresh()}
async function skipTracker(){await post('api/select_tracker',{off:true});refresh()}
applyI18n();refresh();setInterval(refresh,5000);
</script></body></html>"""


async def _ui_status(_request):
    return web.json_response({"bike": _bike_addr, "locked": _locked_addr,
                              "tracker": _tracker_mac, "tracker_off": _tracker_off,
                              "alarm_off": _alarm_off,
                              "device": {"manufacturer": DEVICE["manufacturer"],
                                         "model": DEVICE["model"]},
                              "last": _last})


async def _ui_set_alarm(request):
    global _alarm_off
    data = await request.json()
    _alarm_off = not bool(data.get("on", True))
    _save_cfg()
    if _mqtt is not None:
        publish_alarm_discovery(_mqtt)        # add or remove the HomeKit accessory
        if _alarm_off:
            _alarm["state"] = "disarmed"
            publish_alarm("disarmed")
    log.info("UI: alarm %s", "off" if _alarm_off else "on")
    return web.json_response({"ok": True, "alarm_off": _alarm_off})


async def _ui_alarm(request):
    data = await request.json()
    new = {"DISARM": "disarmed", "ARM_AWAY": "armed_away",
           "ARM_HOME": "armed_home"}.get((data.get("cmd") or "").upper())
    if not new:
        return web.json_response({"ok": False}, status=400)
    _alarm["state"] = new
    _alarm["fired"] = False
    publish_alarm(new)
    return web.json_response({"ok": True, "state": new})


async def _ui_lock(request):
    global _lock_cmd
    data = await request.json()
    _lock_cmd = "ON" if data.get("on") else "OFF"
    log.info("UI: lock command queued: %s", _lock_cmd)
    return web.json_response({"ok": True, "queued": _lock_cmd})


async def _ui_scan(_request):
    try:
        async with _scan_lock, BleakScanner(detection_callback=_record):
            await asyncio.sleep(8)
    except Exception as err:  # noqa: BLE001
        return web.json_response({"error": str(err)}, status=500)
    fresh = [d for d in _discovered.values() if time.time() - d["ts"] < 90]
    fresh.sort(key=lambda d: d["rssi"], reverse=True)
    return web.json_response(fresh)


async def _ui_select_bike(request):
    global _bike_addr, _locked_addr
    data = await request.json()
    _bike_addr = (data.get("address") or "").strip() or None
    _locked_addr = None
    _save_cfg()
    log.info("UI: bike set to %s", _bike_addr)
    return web.json_response({"ok": True, "bike": _bike_addr})


async def _ui_pair(_request):
    if not _bike_addr:
        return web.json_response({"ok": False, "error": "no bike selected"}, status=400)
    ok = await ensure_bonded(_bike_addr)
    _last["bonded"] = ok
    return web.json_response({"ok": bool(ok)})


async def _ui_select_tracker(request):
    global _tracker_mac, _tracker_off
    data = await request.json()
    _tracker_off = bool(data.get("off", False))
    _tracker_mac = None if _tracker_off else ((data.get("module_mac") or "").strip() or None)
    _save_cfg()
    log.info("UI: tracker set to %s (off=%s)", _tracker_mac, _tracker_off)
    return web.json_response({"ok": True, "tracker": _tracker_mac, "off": _tracker_off})


async def start_web() -> None:
    if web is None:
        log.warning("setup UI unavailable (aiohttp missing)")
        return
    app = web.Application()
    app.add_routes([
        web.get("/", lambda r: web.Response(text=INDEX_HTML, content_type="text/html")),
        web.get("/bike.png", lambda r: web.FileResponse("/bike.png")),
        web.get("/api/status", _ui_status),
        web.post("/api/scan", _ui_scan),
        web.post("/api/select_bike", _ui_select_bike),
        web.post("/api/pair", _ui_pair),
        web.post("/api/select_tracker", _ui_select_tracker),
        web.post("/api/set_alarm", _ui_set_alarm),
        web.post("/api/alarm", _ui_alarm),
        web.post("/api/lock", _ui_lock),
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", INGRESS_PORT).start()
    log.info("setup UI listening on :%s", INGRESS_PORT)


async def main() -> None:
    global _mqtt
    _mqtt = make_mqtt()
    log.info("reader v2.0 started (%s, cooldown %ss)",
             _bike_addr or "auto-detect", COOLDOWN)
    publish_status("Starting…", "OFF")
    try:
        await start_web()
    except Exception as err:  # noqa: BLE001 - UI must never block the reader
        log.warning("setup UI failed to start: %s", err)
    # Give the retained alarm state a moment to restore, then assert it so the
    # panel always has a value (defaults to disarmed on a first-ever run).
    await asyncio.sleep(2)
    publish_alarm(_alarm["state"])  # type: ignore[arg-type]
    await start_motion(_mqtt)
    # One-time tracker battery read at startup so the module % is always shown
    # (it then refreshes when the bike is on or the alarm is armed, idle otherwise).
    asyncio.create_task(read_tracker_battery())
    await ble_loop(_mqtt)


if __name__ == "__main__":
    asyncio.run(main())
