#!/usr/bin/env python3
"""One-shot test: read the Bosch eBike battery over a *bonded* BLE link.

Run this on the Proxmox host AFTER pairing + trusting the bike with
bluetoothctl (see README.md). The bike display must be ON so the hub
advertises and accepts the connection.

    python3 bosch_test_read.py
"""

import asyncio

from bleak import BleakClient

ADDRESS = "A4:0D:BC:8A:41:D7"
EB21 = "0000eb21-eaa2-11e9-81b4-2a2ae2dbcce4"


def parse_varints(data: bytes) -> dict[int, int]:
    """Decode the varint fields of the flat eb21 protobuf payload."""
    fields: dict[int, int] = {}
    pos = 0

    def read_varint(d: bytes, p: int) -> tuple[int, int]:
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
            tag, pos = read_varint(data, pos)
            field_number, wire_type = tag >> 3, tag & 7
            if wire_type == 0:
                value, pos = read_varint(data, pos)
                fields[field_number] = value
            elif wire_type == 2:
                length, pos = read_varint(data, pos)
                pos += length
            else:
                break
        except Exception:  # noqa: BLE001
            break
    return fields


async def main() -> None:
    print(f"Connecting to {ADDRESS} (bike display must be ON)...")
    async with BleakClient(ADDRESS, timeout=20.0) as client:
        raw = bytes(await client.read_gatt_char(EB21))
        fields = parse_varints(raw)
        print("raw   :", raw.hex())
        print("fields:", fields)
        if 10 in fields:
            print(f"\n  >>> Battery : {fields[10]}%")
        if 9 in fields:
            print(f"  >>> Odometer: {fields[9] / 1000:.1f} km")
        if 10 not in fields:
            print("\n  (!) No battery field (10) in payload — check the read.")


if __name__ == "__main__":
    asyncio.run(main())
