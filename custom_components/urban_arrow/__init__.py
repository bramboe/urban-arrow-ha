"""Urban Arrow (Bosch Smart System) integration for Home Assistant."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .const import CONF_ADDRESS, DEFAULT_NAME, DOMAIN
from .coordinator import UrbanArrowCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Urban Arrow from a config entry."""
    address = entry.data[CONF_ADDRESS]
    coordinator = UrbanArrowCoordinator(hass, address, entry.title or DEFAULT_NAME)

    # Best effort first read. The bike is only reachable while awake, so we do
    # not block setup on it: the entities are created either way and become
    # available as soon as the first poll succeeds.
    await coordinator.async_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_reload_entry))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok


async def _async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the entry when its title/options change."""
    await hass.config_entries.async_reload(entry.entry_id)
