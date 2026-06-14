#!/usr/bin/env python3
"""Bosch eBike (Urban Arrow) BLE -> MQTT reader for Home Assistant.

Use case: you arrive home, park the bike near this host; the battery % is read.

This mirrors exactly what the official eBike Flow app does (captured with
PacketLogger): it reuses the stored bond (no re-pairing), enables notifications
on the push channel, and subscribes to the battery data stream with a
write-without-response. Battery values then arrive as notifications. This avoids
a blocking encrypted read (which can stall) — it is the app's own method.

The host must be BONDED + trusted with the bike (pair once with bluetoothctl).
It continuously scans; the moment the bike appears it connects, subscribes,
captures the battery, optionally reads the odometer, then disconnects. A
cooldown avoids re-reading while parked. The on-disk bond survives reboots.

Config via environment variables (see the systemd unit):
  BIKE_ADDRESS, MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS,
  SCAN_TIMEOUT, SCAN_GAP, COOLDOWN, STREAM_WAIT
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

# Bosch push channel (notify) + command channel (write-without-response)
PUSH_NOTIFY = "00000011-eaa2-11e9-81b4-2a2ae2dbcce4"
PUSH_CMD = "00000012-eaa2-11e9-81b4-2a2ae2dbcce4"
SUB_BATTERY = bytes([0x10, 0x02, 0x03, 0x07])  # subscribe to stream 7 (battery)
EB21 = "0000eb21-eaa2-11e9-81b4-2a2ae2dbcce4"  # snapshot, used only for odometer

# stream-07 battery attribute ids
ATTR_BATTERY = 0x00BC   # battery 1 SoC
ATTR_BATTERY2 = 0x00CA  # battery 2 / average SoC

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")

SCAN_TIMEOUT = float(os.getenv("SCAN_TIMEOUT", "10"))
SCAN_GAP = float(os.getenv("SCAN_GAP", "3"))
COOLDOWN = float(os.getenv("COOLDOWN", "120"))
STREAM_WAIT = float(os.getenv("STREAM_WAIT", "12"))

DISC_PREFIX = "homeassistant"
NODE = "urban_arrow"
AVAIL_TOPIC = f"{NODE}/availability"
STATE_TOPIC = f"{NODE}/state"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bosch-reader")


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
    client.publish(AVAIL_TOPIC, "online", retain=True)
    _publish_discovery(client)
    log.info("connected to MQTT %s:%s", MQTT_HOST, MQTT_PORT)


def _publish_discovery(client: mqtt.Client) -> None:
    def cfg(obj_id: str, name: str, **extra) -> None:
        payload = {
            "name": name,
            "unique_id": f"{NODE}_{obj_id}",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.%s }}" % obj_id,
            "availability_topic": AVAIL_TOPIC,
            "device": DEVICE,
            **extra,
        }
        client.publish(
            f"{DISC_PREFIX}/sensor/{NODE}/{obj_id}/config", json.dumps(payload), retain=True
        )

    cfg("battery", "Battery", device_class="battery",
        unit_of_measurement="%", state_class="measurement")
    cfg("battery2", "Battery 2", device_class="battery",
        unit_of_measurement="%", state_class="measurement")
    cfg("odometer", "Odometer", device_class="distance",
        unit_of_measurement="km", state_class="total_increasing", icon="mdi:counter")


def make_mqtt() -> mqtt.Client:
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="urban_arrow_reader")
    except AttributeError:  # paho-mqtt < 2.0
        client = mqtt.Client(client_id="urban_arrow_reader")
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.will_set(AVAIL_TOPIC, "offline", retain=True)
    client.on_connect = _on_connect
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


# ----------------------------------------------------------- protobuf (odo)
def _odometer_km(raw: bytes) -> float | None:
    """Extract odometer (field 9, meters) from an eb21 snapshot."""
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
    return round(fields[9] / 1000, 1) if 9 in fields else None


# -------------------------------------------------------------------- BLE
async def read_via_stream(mqtt_client: mqtt.Client, device) -> bool:
    """Connect, subscribe to the battery stream (app method), publish, disconnect."""
    battery: dict[int, int] = {}
    got = asyncio.Event()

    def on_notify(_char, data: bytearray) -> None:
        b = bytes(data)
        # stream-07 battery frame: 30 07 <attr LE16> <counter 3B> 08 <soc>
        if len(b) >= 9 and b[0] == 0x30 and b[1] == 0x07 and b[7] == 0x08:
            attr = b[2] | (b[3] << 8)
            battery[attr] = b[8]
            if attr in (ATTR_BATTERY, ATTR_BATTERY2):
                got.set()

    log.info("connecting to %s ...", ADDRESS)
    async with BleakClient(device, timeout=20.0) as client:
        log.info("connected — subscribing to battery stream")
        await client.start_notify(PUSH_NOTIFY, on_notify)
        await client.write_gatt_char(PUSH_CMD, SUB_BATTERY, response=False)
        try:
            await asyncio.wait_for(got.wait(), timeout=STREAM_WAIT)
        except asyncio.TimeoutError:
            log.warning("no battery notification within %ss", STREAM_WAIT)

        odometer = None
        try:
            raw = bytes(await asyncio.wait_for(client.read_gatt_char(EB21), timeout=8))
            odometer = _odometer_km(raw)
        except Exception as err:  # noqa: BLE001
            log.debug("odometer read skipped: %s", err)

        try:
            await client.stop_notify(PUSH_NOTIFY)
        except Exception:  # noqa: BLE001
            pass

    if not battery:
        return False
    state: dict[str, object] = {"battery": battery.get(ATTR_BATTERY, battery.get(ATTR_BATTERY2))}
    if ATTR_BATTERY in battery and ATTR_BATTERY2 in battery:
        state["battery2"] = battery[ATTR_BATTERY2]
    if odometer is not None:
        state["odometer"] = odometer
    mqtt_client.publish(STATE_TOPIC, json.dumps(state), retain=True)
    log.info("published %s", state)
    return True


async def ble_loop(mqtt_client: mqtt.Client) -> None:
    """Continuously scan; on seeing the bike, read once (then cooldown)."""
    last_ok = 0.0
    while True:
        try:
            if time.time() - last_ok < COOLDOWN:
                await asyncio.sleep(SCAN_GAP)
                continue
            device = await BleakScanner.find_device_by_address(ADDRESS, timeout=SCAN_TIMEOUT)
            if device is not None:
                log.info("bike seen — connecting to read")
                if await read_via_stream(mqtt_client, device):
                    last_ok = time.time()
        except Exception as err:  # noqa: BLE001
            log.warning("read cycle failed: %s", err)
        await asyncio.sleep(SCAN_GAP)


async def main() -> None:
    mqtt_client = make_mqtt()
    log.info("reader started for %s (app stream method, cooldown %ss)", ADDRESS, COOLDOWN)
    try:
        await ble_loop(mqtt_client)
    finally:
        mqtt_client.publish(AVAIL_TOPIC, "offline", retain=True)
        mqtt_client.loop_stop()


if __name__ == "__main__":
    asyncio.run(main())
