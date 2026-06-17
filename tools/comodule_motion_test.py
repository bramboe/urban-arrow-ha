#!/usr/bin/env python3
"""Watch the COMODULE (URBANARROW) accelerometer/status on characteristic 155e.

Use this to (a) find which URBANARROW address is your bike and (b) see what
motion looks like. Run it, then MOVE the switched-off bike and watch the values.

  COMODULE_ADDRESS=FD:F1:8F:61:1D:F7 \
    ~/bosch-reader/venv/bin/python comodule_motion_test.py

155e frames (1 byte 1 = frame type):
  0xC6 = COMODULE status (tracker battery, temperature) — every ~5s, stable
  0xC8 = sensor/accelerometer data — ~1/s, changes when the bike moves
"""

from __future__ import annotations

import asyncio
import os

from bleak import BleakClient, BleakScanner

ADDRESS = os.getenv("COMODULE_ADDRESS", "FD:F1:8F:61:1D:F7")
CHAR_155E = "0000155e-1212-efde-1523-785feabcd123"
WATCH_SECONDS = float(os.getenv("WATCH_SECONDS", "60"))


def on_notify(_char, data: bytearray) -> None:
    b = bytes(data)
    ftype = b[1] if len(b) > 1 else 0
    label = {0xC6: "status", 0xC8: "sensor/motion"}.get(ftype, "?")
    print(f"155e: {b.hex():26s}  type=0x{ftype:02x} ({label})")


async def main() -> None:
    print(f"scanning for {ADDRESS} ...")
    device = await BleakScanner.find_device_by_address(ADDRESS, timeout=20.0)
    if device is None:
        print("not found — is this address advertising? try the other URBANARROW.")
        return
    print("connecting ...")
    async with BleakClient(device, timeout=20.0) as client:
        # initial read
        try:
            raw = bytes(await client.read_gatt_char(CHAR_155E))
            print(f"155e (read): {raw.hex()}")
        except Exception as err:  # noqa: BLE001
            print(f"read failed: {err}")
        await client.start_notify(CHAR_155E, on_notify)
        print(f"subscribed — MOVE THE BIKE now ({WATCH_SECONDS:.0f}s, Ctrl+C to stop)")
        await asyncio.sleep(WATCH_SECONDS)
        await client.stop_notify(CHAR_155E)


if __name__ == "__main__":
    asyncio.run(main())
