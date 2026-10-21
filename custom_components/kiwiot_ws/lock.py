"""Home Assistant lock platform for KiwiOT."""
from __future__ import annotations
from homeassistant.components.lock import LockEntity
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from .const import DOMAIN
from .event_utils import lock_state

class KiwiLockEntity(LockEntity):
    """Expose last known state; keep remote unlock behind existing MFA flow."""
    # has_entity_name with no name/translation_key: the entity is the device
    # itself, so the friendly name is just the device name ("门锁").
    _attr_has_entity_name = True
    _attr_should_poll = False
    def __init__(self, device: object, initial_event: dict | None) -> None:
        self._device = device
        self._event = initial_event or {}
        self._attr_unique_id = f"{DOMAIN}_{device.device_id}_lock"
    @property
    def device_info(self): return self._device.get_device_info()
    @property
    def is_locked(self) -> bool | None: return lock_state(self._event)
    def update_from_event(self, event: dict) -> None:
        if lock_state(event) is not None: self._event = event

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    entities = hass.data[DOMAIN][entry.entry_id].get("entities", [])
    locks = [entity for entity in entities if isinstance(entity, KiwiLockEntity)]
    if locks: async_add_entities(locks, True)
