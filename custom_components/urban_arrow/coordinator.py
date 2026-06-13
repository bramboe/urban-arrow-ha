"""Data update coordinator for the Urban Arrow integration.

Each refresh wakes the bike over BLE (through any in-range ESPHome Bluetooth
proxy), reads the eb21 status characteristic, parses the protobuf varints and
hands the result to the sensors. The connection is opened and closed per poll
so we never hold a scarce proxy connection slot while the bike is idle.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from bleak.backends.device import BLEDevice
from bleak_retry_connector import BleakClientWithServiceCache, establish_connection
from homeassistant.components import bluetooth
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import BATTERY_CHAR_UUID, DOMAIN, UPDATE_INTERVAL_SECONDS
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
        self._services_logged = False

    def _resolve_device(self) -> BLEDevice | None:
        """Return the current BLEDevice for our address, or None if out of range."""
        return bluetooth.async_ble_device_from_address(
            self.hass, self.address, connectable=True
        )

    async def _async_update_data(self) -> dict[int, int]:
        """Connect, read eb21, parse and return the varint fields."""
        device = self._resolve_device()
        if device is None:
            raise UpdateFailed(
                f"Urban Arrow {self.address} not in range of any Bluetooth proxy "
                "(is the bike awake? turn on the display)"
            )

        client: BleakClientWithServiceCache | None = None
        try:
            client = await establish_connection(
                BleakClientWithServiceCache,
                device,
                self.name,
                timeout=CONNECT_TIMEOUT,
            )

            # First successful connect: log the full GATT table once so we can
            # confirm which peripheral/characteristic actually carries the data.
            if not self._services_logged:
                self._log_services(client)
                self._services_logged = True

            char = client.services.get_characteristic(BATTERY_CHAR_UUID)
            if char is None:
                # The Bosch eb21 characteristic is not reachable over the proxy.
                # Probe instead: read every readable characteristic and log the
                # raw bytes (WARNING, so it shows without debug logging) so we
                # can locate the battery in the Urban Arrow custom service.
                await self._probe_readables(client)
                raise UpdateFailed(
                    f"Characteristic {BATTERY_CHAR_UUID} not found on "
                    f"{self.address}; probed readable characteristics (see log)"
                )

            raw = await client.read_gatt_char(char)
            fields = parse_proto_varints(bytes(raw))
            _LOGGER.debug("Urban Arrow %s read: %s", self.address, fields)
            return fields
        except UpdateFailed:
            raise
        except Exception as err:  # noqa: BLE001 - surface any BLE error as a failed update
            raise UpdateFailed(f"Error reading Urban Arrow {self.address}: {err}") from err
        finally:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:  # noqa: BLE001
                    _LOGGER.debug("Disconnect after read failed", exc_info=True)

    async def _probe_readables(self, client: BleakClientWithServiceCache) -> None:
        """Read every readable characteristic and log its raw bytes (WARNING).

        Diagnostic only: lets us find which characteristic on the Urban Arrow
        custom service carries the battery / odometer without debug logging.
        """
        for service in client.services:
            # Skip the generic access/attribute services; they carry no telemetry.
            if service.uuid.startswith(("00001800", "00001801")):
                continue
            for char in service.characteristics:
                if "read" not in char.properties:
                    continue
                try:
                    value = bytes(await client.read_gatt_char(char))
                    _LOGGER.warning(
                        "PROBE %s %s = %s (%d bytes)",
                        self.address,
                        char.uuid,
                        value.hex(),
                        len(value),
                    )
                except Exception as err:  # noqa: BLE001
                    _LOGGER.warning(
                        "PROBE %s %s read failed: %s", self.address, char.uuid, err
                    )

    def _log_services(self, client: BleakClientWithServiceCache) -> None:
        """Dump the GATT services/characteristics to the debug log (once)."""
        for service in client.services:
            _LOGGER.debug("Service %s", service.uuid)
            for char in service.characteristics:
                _LOGGER.debug(
                    "  char %s  props=%s", char.uuid, ",".join(char.properties)
                )

    @staticmethod
    def _summarize_services(client: BleakClientWithServiceCache) -> str:
        """Return a compact one-line summary of the GATT table for diagnostics."""
        parts: list[str] = []
        for service in client.services:
            chars = ", ".join(
                f"{char.uuid}({'/'.join(char.properties)})"
                for char in service.characteristics
            )
            parts.append(f"svc {service.uuid} -> [{chars}]")
        return " ; ".join(parts) if parts else "(no services discovered)"
