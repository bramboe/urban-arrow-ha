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
FIELD_BATTERY = 10

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
ALARM_STATE_TOPIC = f"{NODE}/alarm/state"
ALARM_CMD_TOPIC = f"{NODE}/alarm/cmd"
ARMED_STATES = ("armed_away", "armed_home", "armed_night")

_mqtt: "mqtt.Client | None" = None
# Alarm state machine (HomeKit Security System via MQTT alarm_control_panel).
_alarm: dict[str, object] = {"state": "disarmed", "restored": False, "fired": False}
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
                       "tracker_off": _tracker_off, "alarm_off": _alarm_off}, fh)
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

# Devices seen during scans, for the setup UI: address -> {name,rssi,kind,module_mac,ts}.
_discovered: dict[str, dict] = {}
# Last known values, for the setup UI status panel.
_last: dict[str, object] = {}
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
    elif msg.topic == ALARM_STATE_TOPIC and not _alarm["restored"]:
        # First retained message after (re)connect = restore the previous state.
        _alarm["restored"] = True
        if payload in ARMED_STATES + ("disarmed", "triggered"):
            _alarm["state"] = payload
            log.info("alarm state restored: %s", payload)


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
async def read_push(client: BleakClient) -> tuple[str | None, dict[str, int] | None]:
    """Subscribe to the Bosch push channel and capture the live ride mode and
    the estimated range per mode.

    Best-effort: enables notifications, replays the app's stream subscriptions
    (so the bike pushes the mode (9809) and range (9857) attributes), listens
    briefly, and returns (mode, ranges). Either may be None if nothing arrived.
    """
    latest: dict[str, object] = {"mode": None, "range": None}
    count = {"n": 0}

    def cb(_char, data: bytearray) -> None:
        count["n"] += 1
        b = bytes(data)
        log.debug("push frame %d: %s", count["n"], b.hex())
        m = parse_mode(b)
        if m:
            latest["mode"] = m
        r = parse_range(b)
        if r:
            latest["range"] = r

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
    global _locked_addr
    last_ok = 0.0
    log.info("scanning (%s)", _bike_addr or "auto-detect 'smart system eBike'")
    while True:
        try:
            device = await find_bike(timeout=15.0)
            auto = _bike_addr is None
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
    ev = asyncio.Event()

    def cb(device, adv) -> None:
        if _record(device, adv) != "tracker":
            return
        if _tracker_mac and (_tracker_module_mac(adv) or "").upper() != _tracker_mac.upper():
            return  # not our tracker
        found["device"] = device
        ev.set()

    async with _scan_lock, BleakScanner(detection_callback=cb):
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        except asyncio.TimeoutError:
            pass
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
        if len(data) > 1 and data[1] == FRAME_MOTION:
            now = time.time()
            state["last"] = now
            if not state["on"]:
                state["on"] = True
                state["since"] = now
                publish_motion(True)
                _last["motion"] = True
                log.info("motion: ON")

    while True:
        if _tracker_off:
            await asyncio.sleep(10)
            continue
        target = await find_comodule()
        if target is None:
            await asyncio.sleep(10)
            continue
        try:
            async with BleakClient(target, timeout=20.0) as client:
                await client.start_notify(CHAR_155E, cb)
                log.info("COMODULE motion watcher connected (%s)", target.address)
                _last["tracker_connected"] = True
                while client.is_connected and not _tracker_off:
                    await asyncio.sleep(1)
                    now = time.time()
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


async def start_motion(_mqtt_client: mqtt.Client) -> None:
    """Launch the self-resolving motion watcher (no-op work if disabled)."""
    publish_motion(False)
    asyncio.create_task(motion_watcher())


# ------------------------------------------------------------- setup UI (Ingress)
INDEX_HTML = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>Urban Arrow</title>
<style>
body{font-family:system-ui,sans-serif;margin:0;padding:16px;background:#f5f5f7;color:#111}
.card{background:#fff;border-radius:12px;padding:16px;margin:0 0 16px;box-shadow:0 1px 4px rgba(0,0,0,.08)}
h1{font-size:20px;margin:0 0 14px}h2{font-size:16px;margin:0 0 6px}
.muted{color:#666;font-size:13px}
button{background:#03a9f4;color:#fff;border:0;border-radius:8px;padding:9px 14px;font-size:14px;cursor:pointer;margin:4px 4px 0 0}
button.sec{background:#e0e0e0;color:#222}button:disabled{opacity:.5;cursor:default}
.row{display:flex;align-items:center;gap:8px;padding:9px;border:1px solid #eee;border-radius:8px;margin:6px 0;cursor:pointer}
.row.sel{border-color:#03a9f4;background:#e8f6fe}
.ok{color:#2e7d32;font-weight:600}.bad{color:#c62828;font-weight:600}
.bar{display:flex;gap:16px;flex-wrap:wrap}.kv{font-size:14px}.hidden{display:none}
</style></head><body>
<h1>🚲 Urban Arrow</h1>
<div class=card><h2>Status</h2><div class=bar id=status><span class=muted>laden…</span></div></div>
<div class=card>
  <h2>1. Fiets</h2><p class=muted>Zet het display van de fiets aan en scan.</p>
  <button onclick="scan('bike')">Scan fietsen</button><div id=bikes></div>
  <div id=bikeActions class=hidden><button onclick="selectBike()">Selecteer deze fiets</button></div>
  <div id=pairBox class=hidden style="margin-top:8px">
    <p class=muted>Zet de fiets in <b>pairing mode</b> (display → nieuw apparaat koppelen), klik dan:</p>
    <button id=pairBtn onclick="pair()">Koppel (pair)</button><span id=pairMsg></span></div>
</div>
<div class=card>
  <h2>2. GPS-tracker <span class=muted>(anti-diefstal, optioneel)</span></h2>
  <p class=muted>De tracker is altijd aan. Scan en kies 'm, of sla over.</p>
  <button onclick="scan('tracker')">Scan trackers</button>
  <button class=sec onclick="skipTracker()">Overslaan / uit</button><div id=trackers></div>
  <div id=trackerActions class=hidden><button onclick="selectTracker()">Selecteer deze tracker</button></div>
</div>
<div class=card>
  <h2>3. Alarm <span class=muted>(optioneel — vereist de tracker)</span></h2>
  <p class=muted>HomeKit-beveiligingssysteem op de beweging: <b>Afwezig</b> = hard (push + lampen), <b>Thuis</b> = stil (alleen melding). Uit = alleen de bewegingssensor, geen alarm.</p>
  <button id=alarmBtn onclick="toggleAlarm()">…</button> <span id=alarmState class=muted></span>
</div>
<script>
let pick={bike:null,tracker:null};
const $=s=>document.querySelector(s);
const api=async(p,o)=>(await fetch(p,o)).json();
const fmt=d=>`${d.address} · ${d.rssi} dBm`+(d.module_mac?` · ${d.module_mac}`:'');
async function refresh(){const s=await api('api/status');const L=s.last||{};
  $('#status').innerHTML=[
   `<span class=kv>🔋 ${L.battery??'?'}%</span>`,
   `<span class=kv>⚙️ ${L.mode??'?'}</span>`,
   `<span class=kv>🛡️ ${L.alarm??'?'}</span>`,
   `<span class=kv>🚲 ${s.bike?`<span class=ok>${s.bike}</span>`:'<span class=bad>geen fiets</span>'}</span>`,
   `<span class=kv>📡 ${s.tracker_off?'tracker uit':(L.tracker_connected?'<span class=ok>tracker verbonden</span>':(s.tracker?s.tracker:'tracker auto'))}</span>`,
  ].join('');
  window._alarmOff=s.alarm_off;
  $('#alarmBtn').textContent=s.alarm_off?'Alarm inschakelen':'Alarm uitschakelen';
  $('#alarmState').innerHTML=s.alarm_off?'momenteel <b>uit</b>':'momenteel <span class=ok>aan</span>';}
async function toggleAlarm(){await post('api/set_alarm',{on:window._alarmOff===true});refresh()}
async function scan(kind){const box=kind==='bike'?'#bikes':'#trackers';
  $(box).innerHTML='<span class=muted>scannen… (±8s)</span>';
  const list=await api('api/scan',{method:'POST'});const items=list.filter(d=>d.kind===kind);
  if(!items.length){$(box).innerHTML='<span class=muted>niets gevonden — staat het apparaat aan/in bereik?</span>';return}
  $(box).innerHTML='';items.forEach(d=>{const el=document.createElement('div');el.className='row';
   el.textContent=(d.name||kind)+' — '+fmt(d);
   el.onclick=()=>{pick[kind]=d;[...$(box).children].forEach(c=>c.classList.remove('sel'));el.classList.add('sel');
    $(kind==='bike'?'#bikeActions':'#trackerActions').classList.remove('hidden')};
   $(box).appendChild(el);});}
const post=(p,b)=>api(p,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b||{})});
async function selectBike(){await post('api/select_bike',{address:pick.bike.address});$('#pairBox').classList.remove('hidden');refresh()}
async function pair(){$('#pairBtn').disabled=true;$('#pairMsg').textContent=' koppelen…';
  const r=await post('api/pair');$('#pairBtn').disabled=false;
  $('#pairMsg').innerHTML=r.ok?' <span class=ok>Gekoppeld ✓</span>':' <span class=bad>Mislukt — staat de fiets in pairing mode?</span>';refresh()}
async function selectTracker(){await post('api/select_tracker',{module_mac:pick.tracker.module_mac});refresh()}
async function skipTracker(){await post('api/select_tracker',{off:true});refresh()}
refresh();setInterval(refresh,5000);
</script></body></html>"""


async def _ui_status(_request):
    return web.json_response({"bike": _bike_addr, "locked": _locked_addr,
                              "tracker": _tracker_mac, "tracker_off": _tracker_off,
                              "alarm_off": _alarm_off, "last": _last})


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
        web.get("/api/status", _ui_status),
        web.post("/api/scan", _ui_scan),
        web.post("/api/select_bike", _ui_select_bike),
        web.post("/api/pair", _ui_pair),
        web.post("/api/select_tracker", _ui_select_tracker),
        web.post("/api/set_alarm", _ui_set_alarm),
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
    await ble_loop(_mqtt)


if __name__ == "__main__":
    asyncio.run(main())
