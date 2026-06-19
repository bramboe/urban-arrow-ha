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

DISC_PREFIX = "homeassistant"
NODE = "urban_arrow"
STATE_TOPIC = f"{NODE}/state"
STATUS_TOPIC = f"{NODE}/status"
MODE_TOPIC = f"{NODE}/mode"

_mqtt: "mqtt.Client | None" = None


def publish_status(status: str, present: str = "ON") -> None:
    """Publish a human-readable status + present(ON/OFF) for the status sensors."""
    if _mqtt is not None:
        _mqtt.publish(STATUS_TOPIC, json.dumps({"status": status, "present": present}),
                      retain=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bosch-reader")


# ---------------------------------------------------------------- protobuf
def parse_battery(raw: bytes) -> int | None:
    """Return field 10 (battery %) from an eb21 protobuf snapshot."""
    fields: dict[int, int] = {}
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
                v, pos = rv(raw, pos)
                fields[fn] = v
            elif wt == 2:
                ln, pos = rv(raw, pos)
                pos += ln
            else:
                break
        except Exception:  # noqa: BLE001
            break
    return fields.get(FIELD_BATTERY)


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
    log.info("connected to MQTT %s:%s", MQTT_HOST, MQTT_PORT)


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
async def read_mode(client: BleakClient) -> str | None:
    """Subscribe to the Bosch push channel and capture the live ride mode.

    Best-effort: enables notifications, replays the app's stream subscriptions
    (so the bike pushes mode frames), listens briefly, and returns the last
    decoded mode. Returns None if nothing arrived (e.g. bike idle).
    """
    latest: dict[str, str | None] = {"mode": None}
    count = {"n": 0}

    def cb(_char, data: bytearray) -> None:
        count["n"] += 1
        b = bytes(data)
        if count["n"] <= 10:  # sample the first frames so we can see the framing
            log.info("push frame %d: %s", count["n"], b.hex())
        m = parse_mode(b)
        if m:
            latest["mode"] = m

    try:
        await client.start_notify(PUSH_NOTIFY, cb)
        log.info("subscribed to push channel %s; sending stream subscriptions", PUSH_NOTIFY)
        for sub in range(1, 8):  # 10 02 03 01 .. 07 — the app's stream subscriptions
            try:
                await client.write_gatt_char(PUSH_WRITE, bytes([0x10, 0x02, 0x03, sub]),
                                              response=False)
            except Exception as err:  # noqa: BLE001
                log.debug("sub %d write failed: %s", sub, err)
        await asyncio.sleep(6)
        await client.stop_notify(PUSH_NOTIFY)
        log.info("push channel: %d frame(s) received, mode=%s", count["n"], latest["mode"])
    except Exception as err:  # noqa: BLE001
        log.warning("mode read failed: %s: %s", type(err).__name__, err)
    return latest["mode"]


async def read_snapshot(mqtt_client: mqtt.Client, device) -> bool:
    """Connect, read eb21 battery + push-channel ride mode, publish (retained)."""
    log.info("connecting to %s (%s) ...", device.address, device.name or "?")
    publish_status(f"Connected to {device.name or 'bike'} — reading…", "ON")
    raw: bytes | None = None
    mode: str | None = None
    async with BleakClient(device, timeout=20.0) as client:
        log.info("reading eb21 snapshot ...")
        for attempt in range(3):
            try:
                raw = bytes(await asyncio.wait_for(client.read_gatt_char(EB21), timeout=10))
                break
            except Exception as err:  # noqa: BLE001
                log.warning("read attempt %d -> %s: %s", attempt + 1, type(err).__name__, err)
                await asyncio.sleep(2)
        mode = await read_mode(client)
    if raw is None:
        log.warning("eb21 read failed (bond missing/untrusted? re-pair via bluetoothctl)")
        publish_status("Read failed — bike awake?", "ON")
        return False
    battery = parse_battery(raw)
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
    mqtt_client.publish(STATE_TOPIC, json.dumps(state), retain=True)
    log.info("published %s", state)
    if mode is not None:
        mqtt_client.publish(MODE_TOPIC, json.dumps({"mode": mode}), retain=True)
        log.info("ride mode: %s", mode)
    publish_status(f"Battery {battery}% read at {time.strftime('%Y-%m-%d %H:%M')}", "ON")
    return True


async def find_bike(timeout: float = 15.0):
    """Scan with a FRESH scanner (started+stopped per call, so it can't wedge
    after a connect) and return the bike's BLEDevice, or None."""
    found: dict[str, object] = {}
    ev = asyncio.Event()
    seen: set[str] = set()

    def cb(device, adv) -> None:
        name = device.name or ""
        if AUTO:
            if NAME_MATCH not in name.lower():
                return
            if device.address not in seen:
                seen.add(device.address)
                log.info("bike candidate: %s  '%s'  rssi=%s", device.address, name, adv.rssi)
        elif device.address.upper() != ADDRESS.upper():
            return
        found["device"] = device
        ev.set()

    async with BleakScanner(detection_callback=cb):
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        except asyncio.TimeoutError:
            pass
    return found.get("device")


async def ble_loop(mqtt_client: mqtt.Client) -> None:
    """Scan (fresh each cycle); on detection, bond if needed and read once."""
    last_ok = 0.0
    log.info("scanning (%s)", "auto-detect 'smart system eBike'" if AUTO else ADDRESS)
    while True:
        try:
            device = await find_bike(timeout=15.0)
            if device is None:
                publish_status("Bike not found (off or out of range)", "OFF")
            elif time.time() - last_ok < COOLDOWN:
                # Seen recently; wait out the cooldown before reading again.
                publish_status("Bike in range — waiting (cooldown)", "ON")
            else:
                log.info("bike seen — connecting to read")
                if await ensure_bonded(device.address) and await read_snapshot(mqtt_client, device):
                    last_ok = time.time()
        except Exception as err:  # noqa: BLE001
            log.warning("cycle failed: %s: %s", type(err).__name__, err or "(timeout)")
            publish_status("Connection failed — keep the bike on, retrying…", "ON")
        await asyncio.sleep(SCAN_GAP)


async def main() -> None:
    global _mqtt
    _mqtt = make_mqtt()
    log.info("reader v1.1 started (%s, cooldown %ss)",
             "auto-detect" if AUTO else ADDRESS, COOLDOWN)
    publish_status("Starting…", "OFF")
    await ble_loop(_mqtt)


if __name__ == "__main__":
    asyncio.run(main())
