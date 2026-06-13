"""Sensors for the Urban Arrow integration."""

from __future__ import annotations

from datetime import datetime

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    PERCENTAGE,
    EntityCategory,
    UnitOfLength,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import (
    CONF_ADDRESS,
    DEFAULT_NAME,
    DOMAIN,
    FIELD_BATTERY,
    FIELD_ODOMETER_M,
    FIELD_TIMESTAMP,
    MANUFACTURER,
)
from .coordinator import UrbanArrowCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Urban Arrow sensors from a config entry."""
    coordinator: UrbanArrowCoordinator = hass.data[DOMAIN][entry.entry_id]
    uid = entry.unique_id or entry.entry_id
    device_info = DeviceInfo(
        identifiers={(DOMAIN, uid)},
        name=entry.title or DEFAULT_NAME,
        manufacturer=MANUFACTURER,
        connections={("bluetooth", entry.data[CONF_ADDRESS])},
    )

    async_add_entities(
        [
            UrbanArrowBatterySensor(coordinator, device_info, uid),
            UrbanArrowOdometerSensor(coordinator, device_info, uid),
            UrbanArrowLastUpdatedSensor(coordinator, device_info, uid),
            UrbanArrowConnectionSensor(coordinator, device_info, uid),
        ]
    )


class UrbanArrowEntity(CoordinatorEntity[UrbanArrowCoordinator], SensorEntity):
    """Base entity that ties a sensor to the coordinator and device."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: UrbanArrowCoordinator,
        device_info: DeviceInfo,
        unique_id_prefix: str,
        key: str,
    ) -> None:
        """Initialize the entity."""
        super().__init__(coordinator)
        self._attr_device_info = device_info
        self._attr_unique_id = f"{unique_id_prefix}_{key}"
        self._attr_translation_key = key


class UrbanArrowBatterySensor(UrbanArrowEntity):
    """State of charge of the bike battery."""

    _attr_device_class = SensorDeviceClass.BATTERY
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, coordinator, device_info, unique_id_prefix) -> None:
        """Initialize the battery sensor."""
        super().__init__(coordinator, device_info, unique_id_prefix, "battery")

    @property
    def native_value(self) -> int | None:
        """Return battery percentage."""
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get(FIELD_BATTERY)


class UrbanArrowOdometerSensor(UrbanArrowEntity):
    """Total distance ridden."""

    _attr_device_class = SensorDeviceClass.DISTANCE
    _attr_native_unit_of_measurement = UnitOfLength.KILOMETERS
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_icon = "mdi:counter"

    def __init__(self, coordinator, device_info, unique_id_prefix) -> None:
        """Initialize the odometer sensor."""
        super().__init__(coordinator, device_info, unique_id_prefix, "odometer")

    @property
    def native_value(self) -> float | None:
        """Return odometer in kilometers (payload is in meters)."""
        if not self.coordinator.data:
            return None
        meters = self.coordinator.data.get(FIELD_ODOMETER_M)
        if meters is None:
            return None
        return round(meters / 1000.0, 1)


class UrbanArrowLastUpdatedSensor(UrbanArrowEntity):
    """Timestamp reported by the bike for its last measurement."""

    _attr_device_class = SensorDeviceClass.TIMESTAMP
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:clock-outline"

    def __init__(self, coordinator, device_info, unique_id_prefix) -> None:
        """Initialize the last-updated sensor."""
        super().__init__(coordinator, device_info, unique_id_prefix, "last_updated")

    @property
    def native_value(self) -> datetime | None:
        """Return the bike's reported measurement time (field 11) as UTC."""
        if not self.coordinator.data:
            return None
        ts = self.coordinator.data.get(FIELD_TIMESTAMP)
        if not ts:
            return None
        try:
            return dt_util.utc_from_timestamp(ts)
        except (ValueError, OverflowError, OSError):
            return None


class UrbanArrowConnectionSensor(UrbanArrowEntity):
    """Whether the last poll reached the bike."""

    _attr_device_class = SensorDeviceClass.ENUM
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:bluetooth-connect"
    _attr_options = ["connected", "disconnected"]

    def __init__(self, coordinator, device_info, unique_id_prefix) -> None:
        """Initialize the connection status sensor."""
        super().__init__(
            coordinator, device_info, unique_id_prefix, "connection_status"
        )

    @property
    def available(self) -> bool:
        """This sensor reports reachability, so it is always available."""
        return True

    @property
    def native_value(self) -> str:
        """Return connected when the last poll succeeded."""
        return "connected" if self.coordinator.last_update_success else "disconnected"
