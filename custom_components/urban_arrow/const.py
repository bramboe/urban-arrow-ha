"""Constants for the Urban Arrow integration."""

from __future__ import annotations

DOMAIN = "urban_arrow"

# BLE identifiers — the main battery lives on the Bosch Smart System hub
# (BRC3600), which advertises with this local name. It is only connectable
# while the bike display is on AND no app (eBike Flow / Urban Arrow) is
# connected, because Bosch allows a single connection at a time.
BOSCH_LOCAL_NAME = "smart system eBike"

# eb21 returns a protobuf telemetry snapshot; reading it needs no pairing.
BATTERY_CHAR_UUID = "0000eb21-eaa2-11e9-81b4-2a2ae2dbcce4"

# Config entry keys
CONF_ADDRESS = "address"
CONF_DEVICE_NAME = "device_name"

DEFAULT_NAME = "Urban Arrow"
MANUFACTURER = "Bosch eBike Systems"

# How often to wake the bike and read the battery (seconds).
UPDATE_INTERVAL_SECONDS = 300

# Protobuf field numbers inside the eb21 payload (varint fields only).
FIELD_ODOMETER_M = 9
FIELD_BATTERY = 10
FIELD_TIMESTAMP = 11
