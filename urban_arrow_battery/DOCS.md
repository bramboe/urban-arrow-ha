# Urban Arrow Battery

Reads the battery percentage of an **Urban Arrow** cargo bike (Bosch Smart
System hub, BRC3600) over Bluetooth Low Energy and publishes it to Home
Assistant via MQTT discovery. A device **Urban Arrow** appears with these
entities:

- **Battery** — last measured battery percentage.
- **Last updated** — when that reading was taken.
- **Status** — what the add-on is doing right now: searching, *"Not paired —
  put the bike in PAIRING MODE"*, *"Connected to … — reading"*, or
  *"Battery NN% read at …"*.
- **Awake** — on when the bike is advertising (on / in range), off otherwise.
- **Bluetooth address** — which BLE device was selected (diagnostic).

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

1. Put the bike in **pairing mode** (display → connect a new device) and
   **start the add-on**. By default `bike_address` is empty, so the add-on
   **auto-detects** the hub by its name (`smart system eBike`), pairs it
   automatically (code-free "Just Works" pairing) and trusts the bond — you
   only do this once.
2. After that, whenever the bike is on and in range, the battery is read and
   updated in Home Assistant. The last reading stays shown until the next one
   (it never goes "unavailable").

The selected device's BLE address is published as a diagnostic sensor
**Bluetooth address**, so you can see which bike was picked. If more than one
"smart system eBike" is in range (e.g. a neighbour), the add-on log lists each
candidate with its signal strength — set **bike_address** to the right one to
pin it.

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
