"""Data update coordinator for the Urban Arrow integration.

Each refresh connects to the Bosch Smart System hub over BLE (through any
in-range ESPHome Bluetooth proxy), reads the eb21 telemetry snapshot, decodes
the protobuf varints (battery, odometer, timestamp) and hands the result to the
sensors. The connection is opened and closed per poll so we never hold the
bike's single BLE connection slot — leaving the eBike Flow app free to connect.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from bleak.backends.device import BLEDevice
from bleak_retry_connector import BleakClientWithServiceCache, establish_connection
from homeassistant.components import bluetooth
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import BATTERY_CHAR_UUID, UPDATE_INTERVAL_SECONDS
from .protocol import parse_proto_varints

_LOGGER = logging.getLogger(__name__)

CONNECT_TIMEOUT = 20.0


class UrbanArrowCoordinator(DataUpdateCoordinator[dict[int, int]]):
    """Poll the bike's battery/odometer over BLE."""

    def __init__(self, hass: HomeAssistant, address: str, name: str) -> None:
        """Initialize the coordinator for a single bike at ``address``."""
        super().__init__(
            hass,
            _LOGGER,
            name=name,
            update_interval=timedelta(seconds=UPDATE_INTERVAL_SECONDS),
        )
        self.address = address

    def _resolve_device(self) -> BLEDevice | None:
        """Return the current BLEDevice for our address, or None if out of range."""
        return bluetooth.async_ble_device_from_address(
            self.hass, self.address, connectable=True
        )

    async def _async_update_data(self) -> dict[int, int]:
        """Connect, read eb21, decode and return the varint fields."""
        device = self._resolve_device()
        if device is None:
            raise UpdateFailed(
                f"Bike {self.address} not in range of any Bluetooth proxy. The "
                "Bosch hub only advertises while the display is on and no app "
                "(eBike Flow / Urban Arrow) is connected."
            )

        client: BleakClientWithServiceCache | None = None
        try:
            client = await establish_connection(
                BleakClientWithServiceCache,
                device,
                self.name,
                timeout=CONNECT_TIMEOUT,
            )

            char = client.services.get_characteristic(BATTERY_CHAR_UUID)
            if char is None:
                raise UpdateFailed(
                    f"Characteristic {BATTERY_CHAR_UUID} not found on "
                    f"{self.address}; is this the Bosch hub (smart system eBike)?"
                )

            # eb21 requires an encrypted link. Ask the proxy to establish
            # encryption/bonding first. This only succeeds if the bike allows
            # code-free ("Just Works") pairing; a 12-digit passkey cannot be
            # entered through an ESP32 proxy.
            try:
                await client.pair()
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("pair() failed or unsupported: %s", err)

            try:
                raw = await client.read_gatt_char(char)
            except Exception as err:  # noqa: BLE001
                if "encryption" in str(err).lower() or "authoriz" in str(err).lower():
                    raise UpdateFailed(
                        "eb21 needs an encrypted/bonded link that the ESP32 proxy "
                        "cannot establish (the bike requires its 12-digit pairing "
                        "code). A bonded reader is needed instead of the proxy."
                    ) from err
                raise
            fields = parse_proto_varints(bytes(raw))
            _LOGGER.debug("Urban Arrow %s eb21 fields: %s", self.address, fields)
            return fields
        except UpdateFailed:
            raise
        except Exception as err:  # noqa: BLE001 - surface any BLE error as a failed update
            raise UpdateFailed(f"Error reading bike {self.address}: {err}") from err
        finally:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:  # noqa: BLE001
                    _LOGGER.debug("Disconnect after read failed", exc_info=True)
