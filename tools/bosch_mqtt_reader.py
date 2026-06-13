#!/usr/bin/env python3
"""Bosch eBike (Urban Arrow) BLE -> MQTT reader for Home Assistant.

Runs on a Linux host that is already BONDED + trusted with the bike (pair once
with bluetoothctl). It mimics how the phone stays connected: it CONTINUOUSLY
tries to connect (so it catches the bike's short advertising window on power-up),
then HOLDS the connection and subscribes to eb21 notifications for live updates.
On disconnect it immediately resumes trying. The bond survives reboots, so after
a server restart this service just reconnects automatically — no re-pairing.

Battery / odometer / last_updated are published to MQTT via HA discovery.

Config via environment variables (see the systemd unit):
  BIKE_ADDRESS, MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS,
  RECONNECT_INTERVAL (s, tight retry while disconnected),
  REFRESH_INTERVAL (s, re-read eb21 while connected)
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

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")

RECONNECT_INTERVAL = float(os.getenv("RECONNECT_INTERVAL", "3"))
REFRESH_INTERVAL = float(os.getenv("REFRESH_INTERVAL", "60"))
SCAN_TIMEOUT = float(os.getenv("SCAN_TIMEOUT", "10"))

DISC_PREFIX = "homeassistant"
NODE = "urban_arrow"
AVAIL_TOPIC = f"{NODE}/availability"
STATE_TOPIC = f"{NODE}/state"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bosch-reader")


# ---------------------------------------------------------------- protobuf
def parse_varints(data: bytes) -> dict[int, int]:
    """Decode the varint fields of the flat eb21 protobuf payload."""
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

    while pos < len(data):
        try:
            tag, pos = rv(data, pos)
            fn, wt = tag >> 3, tag & 7
            if wt == 0:
                v, pos = rv(data, pos)
                fields[fn] = v
            elif wt == 2:
                ln, pos = rv(data, pos)
                pos += ln
            else:
                break
        except Exception:  # noqa: BLE001
            break
    return fields


def to_state(fields: dict[int, int]) -> dict | None:
    """Map eb21 fields to the MQTT state payload (only frames with battery)."""
    if 10 not in fields:
        return None
    out: dict[str, object] = {"battery": fields[10]}
    if 9 in fields:
        out["odometer"] = round(fields[9] / 1000, 1)
    if 11 in fields:
        out["last_updated"] = time.strftime(
            "%Y-%m-%dT%H:%M:%S%z", time.localtime(fields[11])
        )
    return out


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
    cfg("odometer", "Odometer", device_class="distance",
        unit_of_measurement="km", state_class="total_increasing", icon="mdi:counter")
    cfg("last_updated", "Last updated", device_class="timestamp")


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


# -------------------------------------------------------------------- BLE
def publish_state(mqtt_client: mqtt.Client, raw: bytes) -> None:
    state = to_state(parse_varints(raw))
    if state:
        mqtt_client.publish(STATE_TOPIC, json.dumps(state), retain=True)
        log.info("published %s", state)


async def hold_connection(mqtt_client: mqtt.Client, client: BleakClient) -> None:
    """Read once, subscribe to eb21 notifications, and hold until disconnect."""
    log.info("connected to bike %s — holding connection", ADDRESS)

    def _on_notify(_char, data: bytearray) -> None:
        publish_state(mqtt_client, bytes(data))

    try:
        publish_state(mqtt_client, bytes(await client.read_gatt_char(EB21)))
    except Exception as err:  # noqa: BLE001
        log.warning("initial read failed: %s", err)

    try:
        await client.start_notify(EB21, _on_notify)
        log.info("subscribed to eb21 notifications")
    except Exception as err:  # noqa: BLE001
        log.warning("could not subscribe to notifications: %s", err)

    # Hold the link; periodically re-read as a fallback if no notifications arrive.
    while client.is_connected:
        await asyncio.sleep(REFRESH_INTERVAL)
        try:
            publish_state(mqtt_client, bytes(await client.read_gatt_char(EB21)))
        except Exception as err:  # noqa: BLE001
            log.debug("refresh read failed: %s", err)
            break


async def ble_loop(mqtt_client: mqtt.Client) -> None:
    """Continuously try to connect (catching the bike's advertising window)."""
    while True:
        try:
            device = await BleakScanner.find_device_by_address(ADDRESS, timeout=SCAN_TIMEOUT)
            if device is None:
                await asyncio.sleep(RECONNECT_INTERVAL)
                continue
            async with BleakClient(device) as client:
                await hold_connection(mqtt_client, client)
            log.info("bike disconnected — retrying")
        except Exception as err:  # noqa: BLE001
            log.debug("connect attempt failed: %s", err)
        await asyncio.sleep(RECONNECT_INTERVAL)


async def main() -> None:
    mqtt_client = make_mqtt()
    log.info("reader started for %s (continuous reconnect)", ADDRESS)
    try:
        await ble_loop(mqtt_client)
    finally:
        mqtt_client.publish(AVAIL_TOPIC, "offline", retain=True)
        mqtt_client.loop_stop()


if __name__ == "__main__":
    asyncio.run(main())
