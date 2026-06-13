"""Diagnostics support for Urban Arrow."""

from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import CONF_ADDRESS, DOMAIN
from .coordinator import UrbanArrowCoordinator


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator: UrbanArrowCoordinator = hass.data[DOMAIN][entry.entry_id]
    return {
        "address": entry.data.get(CONF_ADDRESS),
        "last_update_success": coordinator.last_update_success,
        "data": coordinator.data,
    }
