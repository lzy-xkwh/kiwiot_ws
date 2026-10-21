import importlib.util
import unittest
from pathlib import Path
MODULE = Path(__file__).parent / "custom_components" / "kiwiot_ws" / "event_utils.py"
SPEC = importlib.util.spec_from_file_location("kiwiot_event_utils", MODULE)
EVENT_UTILS = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(EVENT_UTILS)
class EventUtilsTests(unittest.TestCase):
    def test_lock_states(self):
        self.assertIs(EVENT_UTILS.lock_state({"name": "LOCKED"}), True)
        self.assertIs(EVENT_UTILS.lock_state({"name": "UNLOCKED"}), False)
        self.assertIs(EVENT_UTILS.lock_state({"name": "LOCK_INDOOR_LEVER_UNLOCK"}), False)
    def test_non_state_events_do_not_change_state(self):
        self.assertIsNone(EVENT_UTILS.lock_state({"name": "LOCK_LOW_BATTERY"}))
        self.assertIsNone(EVENT_UTILS.lock_state({"name": "VENDOR_ALERT"}))
        self.assertIsNone(EVENT_UTILS.lock_state(None))
    def test_normalization(self):
        self.assertEqual(EVENT_UTILS.event_name({"name": " lock_add_user "}), "LOCK_ADD_USER")
        self.assertEqual(EVENT_UTILS.event_name({"event": "door_not_closed"}), "DOOR_NOT_CLOSED")
        self.assertEqual(EVENT_UTILS.event_name({"name": 42}), "")
    def test_labels(self):
        self.assertEqual(EVENT_UTILS.event_label({"name": "LOCK_LOW_BATTERY"}), "Low battery")
        self.assertEqual(EVENT_UTILS.event_label({"name": "VENDOR_ALERT"}), "Vendor Alert")
        self.assertEqual(EVENT_UTILS.event_label({}), "Unknown")

    def test_extracts_battery_percent_from_nested_payload(self):
        self.assertEqual(
            EVENT_UTILS.extract_battery_percent({"data": {"battery_level": "78%"}}),
            78,
        )
        self.assertEqual(
            EVENT_UTILS.extract_battery_percent({"device": {"battery": {"value": 42.4}}}),
            42,
        )

    def test_ignores_non_numeric_or_invalid_battery_data(self):
        self.assertIsNone(EVENT_UTILS.extract_battery_percent({"level": 2}))
        self.assertIsNone(EVENT_UTILS.extract_battery_percent({"name": "LOCK_LOW_BATTERY"}))
        self.assertIsNone(EVENT_UTILS.extract_battery_percent({"battery": "low"}))
        self.assertIsNone(EVENT_UTILS.extract_battery_percent({"battery_percent": 130}))
        self.assertIsNone(EVENT_UTILS.extract_battery_percent({"battery": True}))

if __name__ == "__main__": unittest.main()
