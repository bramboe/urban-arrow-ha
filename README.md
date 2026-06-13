# Urban Arrow for Home Assistant

A custom integration that exposes an **Urban Arrow** e-bike (Bosch Smart
System) as a device in Home Assistant. It reads the bike over Bluetooth Low
Energy through any [ESPHome Bluetooth proxy](https://esphome.io/components/bluetooth_proxy.html)
in range — no local Bluetooth adapter required.

## Entities

| Entity | Description |
|---|---|
| **Battery** | State of charge (%) |
| **Odometer** | Total distance (km) |
| **Last updated** | Measurement timestamp reported by the bike |
| **Connection status** | Whether the last poll reached the bike |

## How it works

Every 5 minutes the integration connects to the bike's **Bosch Smart System
hub** (it advertises as `smart system eBike`), reads the telemetry
characteristic `0000eb21-eaa2-11e9-81b4-2a2ae2dbcce4`, decodes the protobuf
payload (field 10 = battery %, field 9 = odometer, field 11 = timestamp) and
updates the sensors. No pairing is required for this read. The connection is
closed again immediately so the bike's single BLE slot stays free for the
eBike Flow app.

The Bosch hub only advertises and accepts a connection when **both**:

1. the bike display is **on**, and
2. **no app** (eBike Flow / Urban Arrow) is connected — Bosch allows only one
   BLE connection at a time.

When neither holds, the sensors go *unavailable* and refresh on your next ride.

## Installation (HACS)

1. HACS → ⋯ → **Custom repositories** → add `https://github.com/bramboe/urban-arrow-ha` as an *Integration*.
2. Install **Urban Arrow** and restart Home Assistant.
3. Turn on the bike's display. It should appear under
   **Settings → Devices & Services → Discovered**, or add it manually via
   **+ Add Integration → Urban Arrow**.

## Notes

- Discovery matches the Bosch hub's local name `smart system eBike`.
- The **COMODULE / `URBANARROW`** module that is always visible is a separate
  GPS/anti-theft tracker; it does **not** expose the main battery without an
  (undocumented) authenticated handshake, so this integration does not use it.
- Bosch hubs use a randomised BLE address. If it changes, Home Assistant
  re-discovers the bike and you re-add it.
- For troubleshooting, enable debug logging
  (`logger` → `custom_components.urban_arrow: debug`).
