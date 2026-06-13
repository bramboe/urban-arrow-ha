"""Protobuf decoding for the Urban Arrow / Bosch Smart System status payload.

The eb21 characteristic returns a flat (non-nested) protobuf message. We only
need the varint fields (battery, odometer, timestamp), so this is a minimal
hand-rolled parser rather than a full protobuf dependency.
"""

from __future__ import annotations


def _varint(data: bytes, pos: int) -> tuple[int, int]:
    """Decode a base-128 varint starting at ``pos``; return (value, new_pos)."""
    result, shift = 0, 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, pos


def parse_proto_varints(data: bytes) -> dict[int, int]:
    """Return the varint fields from a flat protobuf message keyed by field number.

    Length-delimited (wire type 2) fields are skipped; any unknown wire type
    ends parsing so a malformed tail can never raise.
    """
    fields: dict[int, int] = {}
    pos = 0
    while pos < len(data):
        try:
            tag, pos = _varint(data, pos)
            field_number, wire_type = tag >> 3, tag & 7
            if wire_type == 0:  # varint
                value, pos = _varint(data, pos)
                fields[field_number] = value
            elif wire_type == 2:  # length-delimited: skip the nested bytes
                length, pos = _varint(data, pos)
                pos += length
            else:  # unknown wire type — stop parsing
                break
        except Exception:  # noqa: BLE001 - defensive: never raise on bad data
            break
    return fields
