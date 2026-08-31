"""ICI MQTT v5 client for real-time Toniebox data (TNG/TB2 only).

Connects to the Tonie Cloud ICI broker via MQTT v5 over WebSocket Secure (WSS)
and receives real-time push updates for battery, online state, and headphones.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import uuid as uuid_lib
from typing import Any, Callable

import paho.mqtt.client as mqtt

from .const import (
    ICI_HOST,
    ICI_PORT,
    ICI_TOPIC_BATTERY,
    ICI_TOPIC_BEDTIME,
    ICI_TOPIC_HEADPHONES,
    ICI_TOPIC_ONLINE,
    ICI_TOPIC_PLAYBACK,
    ICI_TOPIC_SETTINGS,
    ICI_TOPIC_VOLUME,
)

_LOGGER = logging.getLogger(__name__)

# Topics we subscribe to for each Toniebox
_SUBSCRIBE_TOPICS = [
    ICI_TOPIC_BATTERY,
    ICI_TOPIC_ONLINE,
    ICI_TOPIC_HEADPHONES,
    ICI_TOPIC_SETTINGS,
    ICI_TOPIC_PLAYBACK,
    ICI_TOPIC_VOLUME,
    ICI_TOPIC_BEDTIME,
]


class TonieboxIciClient:
    """MQTT v5 client for ICI real-time push data."""

    def __init__(
        self,
        on_message_callback: Callable[[str, str, dict[str, Any]], None],
        loop: asyncio.AbstractEventLoop | None = None,
        on_auth_failed: Callable[[], Any] | None = None,
    ) -> None:
        self._on_message_callback = on_message_callback
        self._loop = loop
        self._on_auth_failed = on_auth_failed
        self._client: mqtt.Client | None = None
        self._connected = False
        self._boxes: list[dict] = []
        self._user_uuid: str | None = None
        self._last_token: str | None = None
        self._auth_failed = False
        self._lifecycle_lock = asyncio.Lock()
        self._lifecycle_generation = 0

    @property
    def connected(self) -> bool:
        """Return True if currently connected to the ICI broker."""
        return self._connected

    async def connect(
        self,
        user_uuid: str,
        access_token: str,
        boxes: list[dict],
    ) -> None:
        """Connect to ICI broker and subscribe to topics for all TNG boxes."""
        generation = self._invalidate_lifecycle()
        tng_boxes = [b for b in boxes if b.get("generation") == "tng"]

        if not tng_boxes:
            _LOGGER.debug("No TNG Tonieboxes found, skipping ICI connection")
            await self._disconnect_generation(generation, "no TNG Tonieboxes")
            return

        if not self._loop:
            self._loop = asyncio.get_running_loop()

        await self._connect_generation(
            generation, user_uuid, access_token, tng_boxes, "connect requested"
        )

    async def _connect_generation(
        self,
        generation: int,
        user_uuid: str,
        access_token: str,
        boxes: list[dict],
        reason: str,
    ) -> None:
        """Replace the MQTT client if this lifecycle request is still current."""
        async with self._lifecycle_lock:
            if generation != self._lifecycle_generation:
                _LOGGER.debug("ICI MQTT %s was superseded before setup", reason)
                return

            self._user_uuid = user_uuid
            self._last_token = access_token
            self._boxes = boxes
            old_client = self._detach_client()
            if old_client:
                await self._stop_client(old_client)

            random_id = str(uuid_lib.uuid4())
            client_id = f"{user_uuid}_ha_toniebox_{random_id}"

            def _setup_and_connect():
                """Set up and return an MQTT client without publishing it."""
                client = mqtt.Client(
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                    client_id=client_id,
                    transport="websockets",
                    protocol=mqtt.MQTTv5,
                )
                client.ws_set_options(path="/")
                # tls_set() never accepts an ssl_context kwarg in any paho-mqtt
                # version — tls_set_context() is the correct API for supplying a
                # pre-built ssl.SSLContext (here from create_default_context()
                # for full cert verification).
                client.tls_set_context(ssl.create_default_context())
                client.username_pw_set(username=user_uuid, password=access_token)
                client.reconnect_delay_set(min_delay=5, max_delay=120)
                client.on_connect = self._on_connect
                client.on_message = self._on_message
                client.on_disconnect = self._on_disconnect
                client.connect_async(ICI_HOST, ICI_PORT, keepalive=60)
                return client

            try:
                client = await self._loop.run_in_executor(None, _setup_and_connect)
            except Exception:
                if generation == self._lifecycle_generation:
                    _LOGGER.warning("Failed to set up ICI broker connection", exc_info=True)
                else:
                    _LOGGER.debug("Superseded ICI MQTT setup failed", exc_info=True)
                return

            if generation != self._lifecycle_generation:
                _LOGGER.debug("Discarding superseded ICI MQTT client after setup")
                await self._stop_client(client)
                return

            self._client = client
            try:
                client.loop_start()
            except Exception:
                if self._client is client:
                    self._detach_client()
                await self._stop_client(client)
                _LOGGER.warning("Failed to start ICI MQTT network loop", exc_info=True)
                return

            _LOGGER.debug(
                "ICI MQTT connection initiated for %d TNG boxes (%s)",
                len(boxes),
                reason,
            )

    async def disconnect(self) -> None:
        """Disconnect from ICI broker."""
        generation = self._invalidate_lifecycle()
        await self._disconnect_generation(generation, "disconnect requested")

    async def _disconnect_generation(self, generation: int, reason: str) -> None:
        """Stop the current client for a non-superseded lifecycle request."""
        async with self._lifecycle_lock:
            if generation != self._lifecycle_generation:
                _LOGGER.debug("ICI MQTT %s was superseded", reason)
                return
            client = self._detach_client()
            if client:
                _LOGGER.debug("Stopping ICI MQTT client (%s)", reason)
                await self._stop_client(client)

    def _detach_client(self) -> mqtt.Client | None:
        """Detach the current client; called only from the event loop."""
        client = self._client
        self._client = None
        self._connected = False
        return client

    async def _stop_client(self, client: mqtt.Client) -> None:
        """Stop one concrete client without consulting mutable lifecycle state."""
        def _stop() -> None:
            try:
                client.disconnect()
            except Exception:
                _LOGGER.debug("Error disconnecting ICI MQTT client", exc_info=True)
            try:
                client.loop_stop()
            except Exception:
                _LOGGER.debug("Error stopping ICI MQTT network loop", exc_info=True)

        await self._loop.run_in_executor(None, _stop)

    async def _handle_auth_failure(self, client: mqtt.Client) -> None:
        """Clean up after an MQTT auth failure and ask for a fresh token now.

        Without this, ICI would sit idle until the next REST poll cycle
        (up to UPDATE_INTERVAL_MINUTES) happened to refresh the token.
        """
        if client is not self._client:
            _LOGGER.debug("Ignoring authentication failure from superseded ICI client")
            return
        self._auth_failed = True
        generation = self._invalidate_lifecycle()
        await self._disconnect_generation(generation, "authentication failed")
        if self._on_auth_failed:
            try:
                result = self._on_auth_failed()
                if result is not None:
                    await result
            except Exception:
                _LOGGER.debug("ICI on_auth_failed callback raised", exc_info=True)

    def publish_command(self, mac: str, command_type: str, payload: dict[str, Any]) -> bool:
        """Publish an app-control command to a Toniebox (QoS 1).

        Mirrors what the official Tonies app sends. Topic:
            external/toniebox/{MAC}/app-control/{command_type}
        The MAC must match the case used for subscriptions (upper-case, as the
        broker delivers state topics on the upper-case MAC). Returns True if the
        publish was handed to paho, False if we're not connected.
        """
        if not self._client or not self._connected:
            _LOGGER.warning("ICI not connected — cannot send command %s", command_type)
            return False
        topic = f"external/toniebox/{mac}/app-control/{command_type}"
        try:
            info = self._client.publish(topic, json.dumps(payload), qos=1)
            _LOGGER.debug("ICI PUBLISH %s: %s (rc=%s)", topic, payload, info.rc)
            return info.rc == mqtt.MQTT_ERR_SUCCESS
        except Exception:
            _LOGGER.warning("Failed to publish ICI command to %s", topic, exc_info=True)
            return False

    async def reconnect(self, new_token: str) -> None:
        """Reconnect with a new access token."""
        if not self._user_uuid or not self._boxes:
            return
        generation = self._invalidate_lifecycle()
        await self._connect_generation(
            generation,
            self._user_uuid,
            new_token,
            self._boxes,
            "reconnect requested",
        )

    def on_token_refreshed(self, new_token: str) -> None:
        """Called by TonieCloudClient when the token is refreshed."""
        if not self._user_uuid or not self._loop:
            return
        asyncio.run_coroutine_threadsafe(
            self._resume_after_token_refresh(new_token), self._loop
        )

    async def _resume_after_token_refresh(self, new_token: str) -> None:
        """Resume ICI on the event loop after authentication refresh."""
        self._last_token = new_token
        self._auth_failed = False
        _LOGGER.debug("ICI access token refreshed; requesting a new connection")
        await self.reconnect(new_token)

    def _invalidate_lifecycle(self) -> int:
        """Invalidate older in-flight lifecycle work on the event loop."""
        self._lifecycle_generation += 1
        return self._lifecycle_generation

    # ── paho-mqtt callbacks (called from network thread) ──────────────────────

    # Reason code strings that indicate an authentication/authorisation failure.
    _AUTH_FAILURE_CODES = frozenset({
        "Bad user name or password",
        "Not authorized",
        "Not Authorized",
    })

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        rc_str = str(reason_code)
        if rc_str in self._AUTH_FAILURE_CODES:
            # This client-local marker is safe to set in the paho thread and lets
            # an immediately following on_disconnect suppress an old-token retry.
            client._ici_auth_failed = True
        if self._loop:
            asyncio.run_coroutine_threadsafe(
                self._handle_connect_result(client, rc_str), self._loop
            )
        if rc_str in self._AUTH_FAILURE_CODES:
            try:
                client.disconnect()
            except Exception:
                _LOGGER.debug(
                    "Error suspending ICI MQTT after authentication failure",
                    exc_info=True,
                )

    async def _handle_connect_result(self, client: mqtt.Client, rc_str: str) -> None:
        """Apply a paho connect callback on the asyncio event loop."""
        if client is not self._client:
            _LOGGER.debug("Ignoring connect result from superseded ICI client")
            return
        if rc_str == "Success":
            self._connected = True
            self._auth_failed = False
            _LOGGER.info("ICI MQTT connected")
            self._subscribe_all(client)
        elif rc_str in self._AUTH_FAILURE_CODES:
            if not self._auth_failed:
                _LOGGER.warning(
                    "ICI MQTT authentication failed (%s). "
                    "Reconnection suspended until the access token is refreshed.",
                    rc_str,
                )
                await self._handle_auth_failure(client)
        else:
            self._connected = False
            _LOGGER.warning("ICI MQTT connection failed: %s", rc_str)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if self._loop:
            asyncio.run_coroutine_threadsafe(
                self._handle_disconnect_result(client, reason_code), self._loop
            )

    async def _handle_disconnect_result(
        self, client: mqtt.Client, reason_code: Any
    ) -> None:
        """Apply a paho disconnect callback on the asyncio event loop."""
        if client is not self._client:
            _LOGGER.debug("Ignoring disconnect from superseded ICI client")
            return
        self._connected = False
        _LOGGER.debug("ICI MQTT disconnected: %s", reason_code)
        if self._auth_failed or getattr(client, "_ici_auth_failed", False):
            _LOGGER.debug("ICI MQTT waiting for a refreshed access token")
            return
        if self._last_token:
            _LOGGER.debug("ICI MQTT scheduling reconnect after unexpected disconnect")
            await self.reconnect(self._last_token)

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        # Topic format: external/toniebox/{MAC}/{subtopic}
        parts = topic.split("/", 3)
        if len(parts) < 4 or parts[0] != "external" or parts[1] != "toniebox":
            return

        mac = parts[2]
        subtopic = parts[3]

        try:
            payload = json.loads(msg.payload.decode("utf-8")) if msg.payload else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            _LOGGER.debug("ICI: unparseable payload on %s", topic)
            return

        _LOGGER.debug("ICI message: %s/%s → %s", mac, subtopic, payload)

        if self._loop and self._on_message_callback:
            self._loop.call_soon_threadsafe(
                self._on_message_callback, mac, subtopic, payload
            )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _subscribe_all(self, client: mqtt.Client) -> None:
        """Subscribe to all relevant topics for all TNG boxes."""
        if client is not self._client:
            return
        for box in self._boxes:
            mac = box.get("macAddress") or box.get("mac_address", "")
            if not mac:
                continue
            name = box.get("name", "?")
            for subtopic in _SUBSCRIBE_TOPICS:
                full_topic = f"external/toniebox/{mac}/{subtopic}"
                client.subscribe(full_topic, qos=1)
                _LOGGER.debug("ICI subscribed: %s (%s)", full_topic, name)
