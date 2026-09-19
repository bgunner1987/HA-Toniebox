"""Device registration regressions, runnable without a Home Assistant install."""

from __future__ import annotations

import enum
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


def _load_modules():
    """Load the real helpers/player with isolated, minimal HA import doubles."""
    modules = {}

    def module(name, **attributes):
        result = types.ModuleType(name)
        result.__dict__.update(attributes)
        modules[name] = result
        return result

    class CoordinatorEntity:
        def __init__(self, coordinator):
            self.coordinator = coordinator

    package_name = "_toniebox_device_info_tests"
    module(package_name, __path__=[])
    module("homeassistant", __path__=[])
    module("homeassistant.helpers", __path__=[])
    module("homeassistant.helpers.device_registry", CONNECTION_NETWORK_MAC="mac")
    module("homeassistant.components", __path__=[])
    module(
        "homeassistant.components.media_player",
        BrowseMedia=object,
        MediaClass=types.SimpleNamespace(TRACK="track", DIRECTORY="directory"),
        MediaPlayerEntity=type("MediaPlayerEntity", (), {}),
        MediaPlayerEntityFeature=enum.IntFlag(
            "MediaPlayerEntityFeature",
            "BROWSE_MEDIA PLAY_MEDIA PLAY PAUSE NEXT_TRACK PREVIOUS_TRACK "
            "VOLUME_SET VOLUME_STEP TURN_OFF TURN_ON",
        ),
        MediaPlayerState=types.SimpleNamespace(
            **{name: name.lower() for name in
               ("BUFFERING", "ON", "OFF", "IDLE", "PAUSED", "PLAYING")}
        ),
        MediaType=types.SimpleNamespace(MUSIC="music"),
    )
    module("homeassistant.config_entries", ConfigEntry=object)
    module("homeassistant.core", HomeAssistant=object)
    module("homeassistant.helpers.entity_platform", AddEntitiesCallback=object)
    module(
        "homeassistant.helpers.update_coordinator",
        CoordinatorEntity=CoordinatorEntity,
    )
    root = Path(__file__).parents[1] / "custom_components/toniebox"
    loaded = {}
    with patch.dict(sys.modules, modules):
        for name in ("const", "device_info", "media_player"):
            spec = importlib.util.spec_from_file_location(
                f"{package_name}.{name}", root / f"{name}.py"
            )
            loaded[name] = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = loaded[name]
            spec.loader.exec_module(loaded[name])
    return loaded["device_info"], loaded["media_player"]


device_info, media_player = _load_modules()


class ModernRegistry:
    """Enforce HA 2026.9's deprecated-key and parent-ID requirements."""

    def __init__(self):
        self.records = {}
        self.writes = 0

    def async_get_device_by_identifier(self, identifier, config_entry_id):
        return self.records.get((config_entry_id, identifier))

    def async_get_or_create(self, *, config_entry_id, **info):
        if "via_device" in info:
            raise RuntimeError("deprecated via_device; use via_device_id instead")
        if "via_device_id" in info:
            if not any(
                device.id == info["via_device_id"]
                for device in self.records.values()
            ):
                raise ValueError("via_device_id must reference an existing device")
        identifier, = info["identifiers"]
        key = config_entry_id, identifier
        device = self.records.get(key)
        if device is None:
            device = types.SimpleNamespace(id=f"registry-id-{len(self.records)}")
            self.records[key] = device
        device.info = info
        self.writes += 1
        return device


def coordinator(entry_id="entry-1", hass=None):
    return types.SimpleNamespace(
        hass=hass if hass is not None else object(),
        entry=types.SimpleNamespace(entry_id=entry_id),
        data={"households": {"household-1": {
            "name": "Family",
            "tonieboxes": {"box-1": {
                "name": "Bedroom", "mac_address": "AA:BB:CC:DD:EE:FF",
                "firmware_version": "1.2.3", "generation": "tng",
            }},
            "creativetonies": {"creative-1": {"name": "Stories"}},
            "contenttonies": {"content-1": {"name": "Songs"}},
            "discs": {"disc-1": {"name": "Adventure"}},
        }}},
    )


class DeviceInfoTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.registry = ModernRegistry()
        self.coordinator = coordinator()
        self.registry_patch = patch.object(
            device_info.dr, "async_get", return_value=self.registry, create=True
        )
        self.registry_patch.start()
        self.addCleanup(self.registry_patch.stop)

    def child_cases(self):
        return (
            (device_info.toniebox_device_info, "box-1", "tb_box-1", "hh_household-1"),
            (device_info.headphones_device_info, "box-1", "tb_box-1_headphones", "tb_box-1"),
            (device_info.creative_tonie_device_info, "creative-1", "ct_creative-1", "hh_household-1"),
            (device_info.content_tonie_device_info, "content-1", "content_content-1", "hh_household-1"),
            (device_info.disc_device_info, "disc-1", "disc_disc-1", "hh_household-1"),
        )

    def test_every_child_uses_existing_registry_id_not_deprecated_tuple(self):
        for factory, child_id, identifier, parent_identifier in self.child_cases():
            with self.subTest(identifier=identifier):
                self.registry.records.clear()
                info = factory(self.coordinator, "household-1", child_id)
                self.assertNotIn("via_device", info)
                self.assertEqual(info["identifiers"], {("toniebox", identifier)})
                parent = self.registry.async_get_device_by_identifier(
                    ("toniebox", parent_identifier), "entry-1"
                )
                self.assertIsNotNone(parent)
                self.assertEqual(info["via_device_id"], parent.id)
                self.registry.async_get_or_create(config_entry_id="entry-1", **info)

    def test_headphones_first_creates_entire_parent_chain(self):
        info = device_info.headphones_device_info(
            self.coordinator, "household-1", "box-1"
        )
        household = self.registry.records[("entry-1", ("toniebox", "hh_household-1"))]
        box = self.registry.records[("entry-1", ("toniebox", "tb_box-1"))]
        self.assertEqual(info["via_device_id"], box.id)
        self.assertEqual(box.info["via_device_id"], household.id)
        self.assertEqual(household.info["name"], "Family")
        self.assertNotIn("via_device_id", household.info)
        self.assertEqual(box.info["name"], "Bedroom")

    def test_existing_devices_are_reused_without_parent_writes(self):
        for factory, child_id, _, _ in self.child_cases():
            info = factory(self.coordinator, "household-1", child_id)
            first = self.registry.async_get_or_create(config_entry_id="entry-1", **info)
            writes = self.registry.writes
            repeated = factory(self.coordinator, "household-1", child_id)
            self.assertEqual(self.registry.writes, writes)
            self.assertEqual(repeated, info)
            second = self.registry.async_get_or_create(config_entry_id="entry-1", **repeated)
            self.assertEqual(first.id, second.id)
        self.assertEqual(len(self.registry.records), 6)

    def test_parent_lookup_is_scoped_to_config_entry(self):
        first = device_info.toniebox_device_info(self.coordinator, "household-1", "box-1")
        other = coordinator(entry_id="entry-2", hass=self.coordinator.hass)
        second = device_info.toniebox_device_info(other, "household-1", "box-1")
        self.assertNotEqual(first["via_device_id"], second["via_device_id"])
        self.assertEqual(
            second["via_device_id"],
            self.registry.records[("entry-2", ("toniebox", "hh_household-1"))].id,
        )

    def test_dynamic_content_in_new_household_creates_parent(self):
        self.coordinator.data["households"]["household-2"] = {
            "name": "New household", "contenttonies": {"new-content": {}}
        }
        info = device_info.content_tonie_device_info(
            self.coordinator, "household-2", "new-content"
        )
        parent = self.registry.records[("entry-1", ("toniebox", "hh_household-2"))]
        self.assertEqual(info["via_device_id"], parent.id)
        self.assertEqual(parent.info["name"], "New household")

    def test_box_metadata_and_identifiers_remain_unchanged(self):
        info = device_info.toniebox_device_info(self.coordinator, "household-1", "box-1")
        self.assertEqual(info["name"], "Bedroom")
        self.assertEqual(info["serial_number"], "box-1")
        self.assertEqual(info["sw_version"], "1.2.3")
        self.assertEqual(info["connections"], {("mac", "aa:bb:cc:dd:ee:ff")})

    def test_older_ha_keeps_legacy_parent_identifiers(self):
        with patch.object(device_info.dr, "async_get", return_value=object()):
            for factory, child_id, _, parent_identifier in self.child_cases():
                info = factory(self.coordinator, "household-1", child_id)
                self.assertEqual(info["via_device"], ("toniebox", parent_identifier))
                self.assertNotIn("via_device_id", info)

    async def test_media_player_setup_before_sensor_platform(self):
        """Reproduce entity_platform passing DeviceInfo to the strict registry."""
        players = []
        hass = types.SimpleNamespace(data={"toniebox": {"entry-1": self.coordinator}})
        self.coordinator.hass = hass

        def add_entities(entities, **kwargs):
            for entity in entities:
                self.registry.async_get_or_create(
                    config_entry_id="entry-1", **entity.device_info
                )
                players.append(entity)

        await media_player.async_setup_entry(hass, self.coordinator.entry, add_entities)
        self.assertEqual(
            {entity._attr_unique_id for entity in players},
            {"ct_creative-1_player", "tb_box-1_player"},
        )
        self.assertEqual(len(self.registry.records), 3)


if __name__ == "__main__":
    unittest.main()
