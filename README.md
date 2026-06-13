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

Every 5 minutes the integration connects to the bike, reads the Bosch status
characteristic (`0000eb21-eaa2-11e9-81b4-2a2ae2dbcce4`), decodes the protobuf
payload and updates the sensors. The connection is closed again immediately so
it never holds a scarce proxy connection slot.

The bike only advertises and accepts connections **while the display is on**.
When it is asleep the sensors go *unavailable* and refresh on your next ride.

## Installation (HACS)

1. HACS → ⋯ → **Custom repositories** → add `https://github.com/bramboe/urban-arrow-ha` as an *Integration*.
2. Install **Urban Arrow** and restart Home Assistant.
3. Turn on the bike's display. It should appear under
   **Settings → Devices & Services → Discovered**, or add it manually via
   **+ Add Integration → Urban Arrow**.

## Notes

- The bike advertises as `URBANARROW` with service UUID `00001580-…`; discovery
  matches on either.
- Bosch bikes use a randomised (static) BLE address. If it ever changes (e.g.
  after the battery is fully disconnected) Home Assistant re-discovers the bike
  and you re-add it.
- First successful connection logs the full GATT table at debug level
  (`logger` → `custom_components.urban_arrow: debug`) — handy to confirm which
  characteristic carries the data.
