# Urban Arrow Battery

Reads the battery percentage of an **Urban Arrow** cargo bike (Bosch Smart
System hub, BRC3600) over Bluetooth Low Energy and publishes it to Home
Assistant via MQTT discovery. A device **Urban Arrow** appears with a
**Battery** sensor and a **Last updated** timestamp.

## Requirements

- Home Assistant on hardware with a **working Bluetooth adapter** (e.g. HA OS
  on a Raspberry Pi or a bare-metal mini-PC). It does **not** work if HA runs
  in a VM without a passed-through Bluetooth adapter — in that case run the
  standalone reader from `tools/` on a Linux host instead.
- The **Mosquitto broker** add-on (or set the `mqtt_*` options manually).
- The bike's BLE address (the hub advertises as `smart system eBike`).

## Why not a normal (HACS) integration?

The Bosch hub only serves its telemetry over an **encrypted, bonded** BLE link.
Home Assistant's Bluetooth stack (local adapter or ESP proxies) cannot create
that bond, so a regular integration cannot read it. This add-on uses the host's
BlueZ stack directly, which can bond and read.

## Setup

1. Find the bike's BLE address. With the bike awake, on the HA host (or via the
   *Advanced SSH & Web Terminal* add-on):
   `bluetoothctl --timeout 15 scan on | grep "smart system eBike"`
2. Set **bike_address** in this add-on's *Configuration* tab.
3. Put the bike in **pairing mode** (display → connect a new device) and
   **start the add-on**. It pairs automatically (this bike uses code-free
   "Just Works" pairing) and trusts the bond — you only do this once.
4. After that, whenever the bike is on and in range, the battery is read and
   updated in Home Assistant. The last reading stays shown until the next one
   (it never goes "unavailable").

## Options

| Option | Description |
|---|---|
| `bike_address` | BLE address of the Bosch hub, e.g. `A4:0D:BC:8A:41:D7` |
| `cooldown` | Minimum seconds between reads while parked (default 120) |
| `mqtt_host` / `mqtt_port` / `mqtt_user` / `mqtt_pass` | Optional MQTT override. Leave empty to use the Mosquitto add-on automatically. |

## Notes

- The bike allows **one** BLE connection at a time. If the eBike Flow / Urban
  Arrow phone app is connected, the add-on cannot read until the phone
  disconnects (e.g. you walk away with the phone).
- The bond survives reboots; no re-pairing needed unless the bike is fully
  power-cycled at the battery.
