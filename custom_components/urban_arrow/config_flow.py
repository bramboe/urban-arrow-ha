"""Config flow for the Urban Arrow integration."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.components import bluetooth
from homeassistant.components.bluetooth import BluetoothServiceInfoBleak

try:  # HA 2024.4+
    from homeassistant.config_entries import ConfigFlowResult
except ImportError:  # pragma: no cover - older cores
    from homeassistant.data_entry_flow import FlowResult as ConfigFlowResult

from .const import (
    CONF_ADDRESS,
    CONF_DEVICE_NAME,
    DEFAULT_NAME,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)


def _format_unique_id(address: str) -> str:
    """Normalise a BLE address for use as the entry unique_id."""
    return address.upper()


def _is_bike(info: BluetoothServiceInfoBleak) -> bool:
    """Return True if a discovery looks like the bike's Bosch hub."""
    return bool(info.name and "smart system" in info.name.lower())


class UrbanArrowConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Urban Arrow."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._discovery_info: BluetoothServiceInfoBleak | None = None
        self._address: str | None = None

    async def async_step_bluetooth(
        self, discovery_info: BluetoothServiceInfoBleak
    ) -> ConfigFlowResult:
        """Handle a bike discovered via Bluetooth."""
        await self.async_set_unique_id(_format_unique_id(discovery_info.address))
        self._abort_if_unique_id_configured()

        self._discovery_info = discovery_info
        self._address = discovery_info.address
        self.context["title_placeholders"] = {
            "name": f"{DEFAULT_NAME} ({discovery_info.address})"
        }
        return await self.async_step_confirm()

    async def async_step_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Confirm a discovered bike and optionally name it."""
        assert self._address is not None
        if user_input is not None:
            name = (user_input.get(CONF_DEVICE_NAME) or "").strip() or DEFAULT_NAME
            return self.async_create_entry(
                title=name,
                data={CONF_ADDRESS: self._address},
            )

        return self.async_show_form(
            step_id="confirm",
            data_schema=vol.Schema(
                {vol.Optional(CONF_DEVICE_NAME, default=DEFAULT_NAME): str}
            ),
            description_placeholders={"address": self._address},
        )

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Let the user pick a discovered bike (or enter an address manually)."""
        if user_input is not None:
            address = user_input[CONF_ADDRESS]
            if address == "manual":
                return await self.async_step_manual()
            await self.async_set_unique_id(
                _format_unique_id(address), raise_on_progress=False
            )
            self._abort_if_unique_id_configured()
            self._address = address
            return await self.async_step_confirm()

        configured = {
            entry.unique_id
            for entry in self._async_current_entries()
            if entry.unique_id
        }
        choices: dict[str, str] = {}
        for info in bluetooth.async_discovered_service_info(self.hass, connectable=True):
            if not _is_bike(info):
                continue
            if _format_unique_id(info.address) in configured:
                continue
            choices[info.address] = f"{info.name or DEFAULT_NAME} ({info.address})"

        choices["manual"] = "Enter address manually"
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({vol.Required(CONF_ADDRESS): vol.In(choices)}),
        )

    async def async_step_manual(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle manual BLE address entry."""
        errors: dict[str, str] = {}
        if user_input is not None:
            raw = user_input[CONF_ADDRESS].upper().replace(" ", "").replace(":", "")
            if len(raw) != 12 or not all(c in "0123456789ABCDEF" for c in raw):
                errors["base"] = "invalid_address"
            else:
                address = ":".join(raw[i : i + 2] for i in range(0, 12, 2))
                await self.async_set_unique_id(
                    _format_unique_id(address), raise_on_progress=False
                )
                self._abort_if_unique_id_configured()
                self._address = address
                return await self.async_step_confirm()

        return self.async_show_form(
            step_id="manual",
            data_schema=vol.Schema({vol.Required(CONF_ADDRESS): str}),
            errors=errors,
        )
