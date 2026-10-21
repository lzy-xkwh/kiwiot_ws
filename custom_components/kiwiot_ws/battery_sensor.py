"""Battery sensor platform support for KiwiOT locks."""
from __future__ import annotations

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.const import PERCENTAGE
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .event_utils import extract_battery_percent


class KiwiLockBatterySensor(SensorEntity):
    """Expose the last valid battery percentage reported by KiwiOT."""

    _attr_has_entity_name = True
    _attr_translation_key = "battery"
    _attr_should_poll = False
    _attr_device_class = SensorDeviceClass.BATTERY
    _attr_native_unit_of_measurement = PERCENTAGE

    def __init__(self, device: object, initial_data: dict) -> None:
        self._device = device
        self._attr_unique_id = f"{DOMAIN}_{device.device_id}_battery"
        self._attr_native_value = extract_battery_percent(initial_data)

    @property
    def device_info(self):
        return self._device.get_device_info()

    def update_from_payload(self, payload: dict) -> bool:
        """Update value if payload contains a valid battery percentage."""
        value = extract_battery_percent(payload)
        if value is None or value == self._attr_native_value:
            return False
        self._attr_native_value = value
        return True


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    entities = hass.data[DOMAIN][entry.entry_id].get("entities", [])
    sensors = [entity for entity in entities if isinstance(entity, KiwiLockBatterySensor)]
    if sensors:
        async_add_entities(sensors, True)
