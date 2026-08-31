"""Regression tests for the ICI MQTT client lifecycle."""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import ssl
import sys
import threading
import types
import unittest
from unittest.mock import patch


class FakeMqttClient:
    """Small paho client double with optional blocking construction."""

    created: list["FakeMqttClient"] = []
    construction_started: threading.Event | None = None
    construction_release: threading.Event | None = None

    def __init__(self, **kwargs):
        del kwargs
        type(self).created.append(self)
        if type(self).construction_started:
            type(self).construction_started.set()
        if type(self).construction_release:
            type(self).construction_release.wait(timeout=5)
        self.loop_started = False
        self.loop_stopped = False
        self.disconnected = False
        self.username = None
        self.password = None
        self.subscriptions = []

    def ws_set_options(self, **kwargs):
        pass

    def tls_set_context(self, context):
        pass

    def username_pw_set(self, username, password):
        self.username = username
        self.password = password

    def reconnect_delay_set(self, **kwargs):
        pass

    def connect_async(self, *args, **kwargs):
        pass

    def loop_start(self):
        self.loop_started = True

    def loop_stop(self):
        self.loop_stopped = True

    def disconnect(self):
        self.disconnected = True

    def subscribe(self, topic, qos):
        self.subscriptions.append((topic, qos))


def _load_ici_module():
    """Load ici_client without importing Home Assistant's package __init__."""
    mqtt_module = types.ModuleType("paho.mqtt.client")
    mqtt_module.Client = FakeMqttClient
    mqtt_module.CallbackAPIVersion = types.SimpleNamespace(VERSION2=2)
    mqtt_module.MQTTv5 = 5
    mqtt_module.MQTT_ERR_SUCCESS = 0
    sys.modules.setdefault("paho", types.ModuleType("paho"))
    sys.modules.setdefault("paho.mqtt", types.ModuleType("paho.mqtt"))
    sys.modules["paho.mqtt.client"] = mqtt_module

    package = types.ModuleType("custom_components.toniebox")
    package.__path__ = []
    sys.modules["custom_components.toniebox"] = package
    const = types.ModuleType("custom_components.toniebox.const")
    const.ICI_HOST = "example.invalid"
    const.ICI_PORT = 443
    for name in (
        "BATTERY",
        "BEDTIME",
        "HEADPHONES",
        "ONLINE",
        "PLAYBACK",
        "SETTINGS",
        "VOLUME",
    ):
        setattr(const, f"ICI_TOPIC_{name}", name.lower())
    sys.modules[const.__name__] = const

    path = Path(__file__).parents[1] / "custom_components/toniebox/ici_client.py"
    spec = importlib.util.spec_from_file_location(
        "custom_components.toniebox.ici_client", path
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ICI = _load_ici_module()
BOXES = [{"generation": "tng", "macAddress": "AABB", "name": "Box"}]


class IciLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        FakeMqttClient.created.clear()
        FakeMqttClient.construction_started = None
        FakeMqttClient.construction_release = None
        self.ssl_patch = patch.object(ssl, "create_default_context", return_value=object())
        self.ssl_patch.start()

    async def asyncTearDown(self):
        self.ssl_patch.stop()

    def make_client(self, auth_callback=None):
        return ICI.TonieboxIciClient(
            lambda *args: None,
            loop=asyncio.get_running_loop(),
            on_auth_failed=auth_callback,
        )

    async def test_disconnect_while_connect_setup_is_in_flight(self):
        started = threading.Event()
        release = threading.Event()
        FakeMqttClient.construction_started = started
        FakeMqttClient.construction_release = release
        ici = self.make_client()

        connect_task = asyncio.create_task(ici.connect("user", "old", BOXES))
        await asyncio.to_thread(started.wait, 2)
        disconnect_task = asyncio.create_task(ici.disconnect())
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(connect_task, disconnect_task)

        old = FakeMqttClient.created[0]
        self.assertIsNone(ici._client)
        self.assertFalse(old.loop_started)
        self.assertTrue(old.loop_stopped)
        self.assertTrue(old.disconnected)

    async def test_auth_failure_waits_for_refresh_and_uses_new_token(self):
        ici = None

        def refresh_token():
            ici.on_token_refreshed("new")

        ici = self.make_client(refresh_token)
        await ici.connect("user", "old", BOXES)
        old = ici._client
        ici._on_connect(
            old, None, None, "Bad user name or password", None
        )
        ici._on_disconnect(old, None, None, "Not authorized", None)
        for _ in range(100):
            if ici._client is not None and ici._client is not old:
                break
            await asyncio.sleep(0.01)

        self.assertTrue(old.loop_stopped)
        self.assertIsNot(ici._client, old)
        self.assertEqual(ici._client.password, "new")
        self.assertEqual(self.active_clients(), [ici._client])

    async def test_concurrent_reconnects_leave_one_active_client(self):
        ici = self.make_client()
        await ici.connect("user", "old", BOXES)
        await asyncio.gather(*(ici.reconnect("new") for _ in range(5)))

        self.assertEqual(self.active_clients(), [ici._client])
        self.assertEqual(ici._client.password, "new")

    async def test_expiry_disconnect_refreshes_before_reconnect(self):
        refresh_calls = 0
        ici = None

        def refresh_token():
            nonlocal refresh_calls
            refresh_calls += 1
            ici.on_token_refreshed("token-b")

        ici = self.make_client(refresh_token)
        await ici.connect("user", "token-a", BOXES)
        old = ici._client
        await ici._handle_disconnect_result(old, "Normal disconnection")
        await self.wait_for_new_client(ici, old)

        self.assertIsNot(ici._client, old)
        self.assertTrue(old.loop_stopped)
        self.assertEqual(refresh_calls, 1)
        self.assertEqual(ici._client.password, "token-b")
        self.assertEqual(
            [client.password for client in FakeMqttClient.created],
            ["token-a", "token-b"],
        )
        self.assertEqual(self.active_clients(), [ici._client])

    async def test_concurrent_rest_refresh_supersedes_disconnect_work(self):
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()

        async def refresh_token():
            refresh_started.set()
            await release_refresh.wait()

        ici = self.make_client(refresh_token)
        await ici.connect("user", "token-a", BOXES)
        old = ici._client
        disconnect_task = asyncio.create_task(
            ici._handle_disconnect_result(old, "network error")
        )
        await refresh_started.wait()

        ici.on_token_refreshed("token-b")
        await self.wait_for_new_client(ici, old)
        release_refresh.set()
        await disconnect_task

        self.assertEqual(ici._client.password, "token-b")
        self.assertEqual(self.active_clients(), [ici._client])

    async def test_fresh_token_rejection_remains_an_auth_failure(self):
        refresh_calls = 0
        ici = None

        def refresh_token():
            nonlocal refresh_calls
            refresh_calls += 1
            if refresh_calls == 1:
                ici.on_token_refreshed("token-b")

        ici = self.make_client(refresh_token)
        await ici.connect("user", "token-a", BOXES)
        old = ici._client
        await ici._handle_disconnect_result(old, "Normal disconnection")
        await self.wait_for_new_client(ici, old)
        fresh = ici._client

        with self.assertLogs(ICI._LOGGER, level="WARNING") as logs:
            await ici._handle_connect_result(fresh, "Bad user name or password")

        self.assertTrue(any("authentication failed" in line for line in logs.output))
        self.assertEqual(refresh_calls, 2)
        self.assertIsNone(ici._client)

    async def test_unexpected_network_disconnect_refreshes_and_reconnects(self):
        ici = None

        def refresh_token():
            ici.on_token_refreshed("network-token")

        ici = self.make_client(refresh_token)
        await ici.connect("user", "old-token", BOXES)
        old = ici._client
        await asyncio.gather(
            ici._handle_disconnect_result(old, "network error"),
            ici._handle_disconnect_result(old, "network error"),
        )
        await self.wait_for_new_client(ici, old)

        self.assertEqual(ici._client.password, "network-token")
        self.assertEqual(self.active_clients(), [ici._client])

    async def test_intentional_disconnect_does_not_reconnect(self):
        ici = self.make_client()
        await ici.connect("user", "token", BOXES)
        old = ici._client
        await ici.disconnect()
        count = len(FakeMqttClient.created)
        await ici._handle_disconnect_result(old, "Success")

        self.assertIsNone(ici._client)
        self.assertEqual(len(FakeMqttClient.created), count)

    @staticmethod
    def active_clients():
        return [
            client
            for client in FakeMqttClient.created
            if client.loop_started and not client.loop_stopped
        ]

    async def wait_for_new_client(self, ici, old):
        for _ in range(100):
            if ici._client is not None and ici._client is not old:
                return
            await asyncio.sleep(0.01)
        self.fail("A refreshed MQTT client was not activated")


if __name__ == "__main__":
    unittest.main()
