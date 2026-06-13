"""Constants for the Urban Arrow integration."""

from __future__ import annotations

DOMAIN = "urban_arrow"

# BLE identifiers
# The bike advertises as "URBANARROW" with this 16-bit service UUID.
SERVICE_UUID = "00001580-0000-1000-8000-00805f9b34fb"
LOCAL_NAME = "URBANARROW"
# Bosch Smart System characteristic that returns the status protobuf.
BATTERY_CHAR_UUID = "0000eb21-eaa2-11e9-81b4-2a2ae2dbcce4"

# Config entry keys
CONF_ADDRESS = "address"
CONF_DEVICE_NAME = "device_name"

DEFAULT_NAME = "Urban Arrow"
MANUFACTURER = "Urban Arrow"

# How often to wake the bike and read the battery (seconds).
UPDATE_INTERVAL_SECONDS = 300

# Protobuf field numbers inside the eb21 payload (varint fields only).
FIELD_ODOMETER_M = 9
FIELD_BATTERY = 10
FIELD_TIMESTAMP = 11
