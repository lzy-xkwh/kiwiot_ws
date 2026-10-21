"""Helpers for interpreting KiwiOT lock events."""
from __future__ import annotations
from typing import Any

EVENT_LABELS = {
    "UNLOCKED": "Unlocked", "LOCKED": "Locked", "REMOTE_UNLOCK": "Remote unlock",
    "LOCK_INDOOR_BUTTON_UNLOCK": "Indoor button unlock",
    "LOCK_INDOOR_LEVER_UNLOCK": "Indoor lever unlock",
    "LOCK_ADD_USER": "User added", "LOCK_DELETE_USER": "User removed",
    "LOCK_PASSWORD_ERROR": "Invalid password",
    "LOCK_FINGERPRINT_ERROR": "Invalid fingerprint",
    "LOCK_FINGERPRINT_FRONZEN": "Fingerprint frozen",
    "LOCK_CARD_ERROR": "Invalid card", "LOCK_TAMPER_ALARM": "Tamper alarm",
    "LOCK_LOW_BATTERY": "Low battery", "LOCK_BATTERY_LOW": "Low battery",
    "DOOR_NOT_CLOSED": "Door not closed", "HUMAN_WANDERING": "Person detected",
}
LOCKED_EVENTS = {"LOCKED"}
UNLOCKED_EVENTS = {"UNLOCKED", "REMOTE_UNLOCK", "LOCK_INDOOR_BUTTON_UNLOCK", "LOCK_INDOOR_LEVER_UNLOCK"}

def event_name(event: dict[str, Any] | None) -> str:
    if not isinstance(event, dict): return ""
    name = event.get("name") or event.get("event") or ""
    return name.strip().upper() if isinstance(name, str) else ""

def lock_state(event: dict[str, Any] | None) -> bool | None:
    name = event_name(event)
    if name in LOCKED_EVENTS: return True
    if name in UNLOCKED_EVENTS: return False
    return None

def event_label(event: dict[str, Any] | None) -> str:
    name = event_name(event)
    return EVENT_LABELS.get(name, name.replace("_", " ").title() if name else "Unknown")


def extract_battery_percent(payload: Any) -> int | None:
    """Extract a plausible percentage from common KiwiOT battery fields.

    Generic keys such as ``level`` are intentionally excluded so event
    severity can never be mistaken for a battery percentage.
    """
    keys = {
        "battery", "battery_level", "battery_percent", "battery_percentage",
        "batteryvalue", "battery_value", "batterylevel", "batterypercent",
        "batterypercentage",
    }

    def parse(value: Any) -> int | None:
        if isinstance(value, dict):
            for nested_key in ("value", "level", "percent", "percentage"):
                if nested_key in value:
                    result = parse(value[nested_key])
                    if result is not None:
                        return result
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            number = value
        elif isinstance(value, str):
            try:
                number = float(value.strip().removesuffix("%"))
            except ValueError:
                return None
        else:
            return None
        return round(number) if 0 <= number <= 100 else None

    def walk(value: Any) -> int | None:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = key.lower().replace("-", "_") if isinstance(key, str) else ""
                if normalized in keys:
                    result = parse(child)
                    if result is not None:
                        return result
            for child in value.values():
                result = walk(child)
                if result is not None:
                    return result
        elif isinstance(value, list):
            for child in value:
                result = walk(child)
                if result is not None:
                    return result
        return None

    return walk(payload)
