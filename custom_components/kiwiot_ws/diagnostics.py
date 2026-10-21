"""Diagnostic support for KiwiOT config entries."""
from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN

TO_REDACT = {
    "identifier", "credential", "X-Kiwik-Client-Id", "client_id",
    "access_token", "refresh_token", "secureToken", "password", "unlock_data",
}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return support diagnostics without account or control secrets."""
    domain_data = hass.data.get(DOMAIN, {})
    runtime = domain_data.get(entry.entry_id, {})
    # Existing integration releases store these runtime values at domain level.
    devices = runtime.get("devices", domain_data.get("devices", {}))
    entities = runtime.get("entities", [])
    if not entities:
        entities = [entity for values in devices.values() for entity in values]
    ws = runtime.get("ws", domain_data.get("ws"))
    return async_redact_data(
        {
            "config_entry": dict(entry.data),
            "device_count": len(devices),
            "device_ids": sorted(devices),
            "websocket_connected": bool(ws and not ws.closed),
            "entities": len(entities),
        },
        TO_REDACT,
    )
