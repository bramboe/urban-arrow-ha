#!/usr/bin/env python3
"""Urban Arrow (Bosch eBike) battery -> MQTT reader for Home Assistant. v1.

Use case: you arrive home, park the bike near this host; the battery % is read.

The host must be BONDED + TRUSTED with the bike (pair once with bluetoothctl).
A persistent scanner waits for the bike to advertise; on detection it connects,
reads the eb21 telemetry snapshot, publishes the battery % to MQTT, and
disconnects. The value is published RETAINED with no availability topic, so the
last reading (plus a "last updated" timestamp) stays visible in Home Assistant
until a new reading arrives — it never goes "unavailable". The on-disk bond
survives reboots, so no re-pairing after a restart.

v1 exposes only the battery and when it was last read. (Odometer / mode /
motion are separate, still-being-reverse-engineered, and not in v1.)

Config via environment variables (see the systemd unit):
  BIKE_ADDRESS, MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS, COOLDOWN, OP_TIMEOUT
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import paho.mqtt.client as mqtt
from bleak import BleakClient, BleakScanner

ADDRESS = os.getenv("BIKE_ADDRESS", "A4:0D:BC:8A:41:D7")
EB21 = "0000eb21-eaa2-11e9-81b4-2a2ae2dbcce4"
FIELD_BATTERY = 10

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")

COOLDOWN = float(os.getenv("COOLDOWN", "120"))
OP_TIMEOUT = float(os.getenv("OP_TIMEOUT", "15"))

DISC_PREFIX = "homeassistant"
NODE = "urban_arrow"
STATE_TOPIC = f"{NODE}/state"

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

    # Remove sensors published by earlier versions (clears stale "unavailable"
    # entities from the broker's retained config topics).
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


# -------------------------------------------------------------------- BLE
async def read_snapshot(mqtt_client: mqtt.Client, device) -> bool:
    """Connect, read eb21, publish battery (retained), disconnect."""
    log.info("connecting to %s ...", ADDRESS)
    async with BleakClient(device, timeout=20.0) as client:
        log.info("reading eb21 snapshot ...")
        raw: bytes | None = None
        for attempt in range(3):
            try:
                raw = bytes(await asyncio.wait_for(client.read_gatt_char(EB21), timeout=10))
                break
            except Exception as err:  # noqa: BLE001
                log.warning("read attempt %d -> %s: %s", attempt + 1, type(err).__name__, err)
                await asyncio.sleep(2)
    if raw is None:
        log.warning("eb21 read failed (bond missing/untrusted? re-pair via bluetoothctl)")
        return False
    battery = parse_battery(raw)
    if battery is None:
        log.warning("no battery field in payload: %s", raw.hex())
        return False
    state = {
        "battery": battery,
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    mqtt_client.publish(STATE_TOPIC, json.dumps(state), retain=True)
    log.info("published %s", state)
    return True


async def ble_loop(mqtt_client: mqtt.Client) -> None:
    """Persistent scanner; on detection, read once (then cooldown)."""
    last_ok = 0.0
    detected: asyncio.Queue = asyncio.Queue()

    def on_detect(device, _adv) -> None:
        if device.address.upper() == ADDRESS.upper():
            try:
                detected.put_nowait(device)
            except asyncio.QueueFull:
                pass

    scanner = BleakScanner(detection_callback=on_detect)
    await scanner.start()
    log.info("scanning for %s", ADDRESS)
    try:
        while True:
            device = await detected.get()
            while not detected.empty():
                detected.get_nowait()
            if time.time() - last_ok < COOLDOWN:
                continue
            log.info("bike seen — connecting to read")
            await scanner.stop()
            try:
                if await read_snapshot(mqtt_client, device):
                    last_ok = time.time()
            except Exception as err:  # noqa: BLE001
                log.warning("read cycle failed: %s", err)
            await scanner.start()
    finally:
        await scanner.stop()


async def main() -> None:
    mqtt_client = make_mqtt()
    log.info("reader v1 started for %s (battery only, cooldown %ss)", ADDRESS, COOLDOWN)
    await ble_loop(mqtt_client)


if __name__ == "__main__":
    asyncio.run(main())
