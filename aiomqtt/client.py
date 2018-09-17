import asyncio
import functools
import logging
import socket
from asyncio import (CancelledError, iscoroutinefunction,
                     run_coroutine_threadsafe)
from typing import Any, List

from paho.mqtt.client import MQTT_ERR_SUCCESS
from paho.mqtt.client import Client as _Client
from paho.mqtt.client import WebsocketConnectionError


def wrap_callback(func):
    orig_prop = getattr(_Client, func.__name__)
    prop = property(None, None, None, orig_prop.__doc__)
    internal_name = "_" + func.__name__

    @prop.getter
    def prop(self):
        return getattr(self, internal_name, None)

    @prop.setter
    def prop(self, value):
        setattr(self, internal_name, value)

        if iscoroutinefunction(value):
            def wrapper(_client, *args):
                logging.debug("Calling coroutine: %s", func.__name__)
                run_coroutine_threadsafe(value(self, *args), self._loop)
        else:
            def wrapper(_client, *args):
                logging.debug("Calling callback: %s", func.__name__)
                self._loop.call_soon_threadsafe(value, self, *args)
        if value is None:
            setattr(self._client, func.__name__, None)
        else:
            setattr(self._client, func.__name__, wrapper)
    return prop


def internal_callback(register_list):
    def decorator(func):
        orig_prop = prop = wrap_callback(func)
        # override setter to call internal callback

        @prop.setter
        def prop(self, value):
            orig_prop.__set__(self, value)
            wrapper = getattr(self._client, func.__name__)
            if wrapper is None:
                def wrapper2(_client, *args):
                    logging.debug(
                        "Calling internal callback: %s", func.__name__)
                    self._loop.call_soon_threadsafe(func, self, *args)
                setattr(self._client, func.__name__, wrapper)
            else:
                def wrapper2(_client, *args):
                    logging.debug(
                        "Calling internal callback: %s", func.__name__)
                    self._loop.call_soon_threadsafe(func, self, *args)
                    wrapper(_client, *args)
                setattr(self._client, func.__name__, wrapper2)

        def register(self):
            def wrapper2(_client, *args):
                self._loop.call_soon_threadsafe(func, self, *args)
            setattr(self._client, func.__name__, wrapper2)
        register_list.append(register)

        return prop
    return decorator


class MQTTMessageInfo(object):
    __slots__ = ["client", "_mqtt_message_info"]

    def __init__(self, client, mqtt_message_info):
        self.client = client
        self._mqtt_message_info = mqtt_message_info

    def __getattr__(self, name):
        return getattr(self._mqtt_message_info, name)

    async def wait_for_publish(self):
        if self._mqtt_message_info.is_published():
            return
        # Sepcial consideration if message is published in the
        # "reconnect" thread.  If is_published is false,
        # then the on_publish callback will be scheduled using
        # "call_soon" and guaranteed to run AFTER this coroutine.
        mid = self._mqtt_message_info.mid
        event = asyncio.Event()
        self.client.waiting[mid] = event
        await event.wait()
        del self.client.waiting[mid]

    def __iter__(self):
        return iter(self._mqtt_message_info)

    def __getitem__(self, i):
        return self._mqtt_message_info[i]

    def __str__(self):
        return str(self._mqtt_message_info)


class Client(object):
    """
    An AsyncIO based wrapper around the paho-mqtt MQTT client class.

    Essentially, the differences between this and the paho.mqtt.client.Client
    are:

    * The constructor takes an asyncio loop to use as the first argument.
    * Blocking methods (connect, connect_srv, reconnect_delay_set) are now
      coroutines.
    * Callback functions are always safely inserted into the asyncio event loop
      rather than being run from an unspecified thread, however the loop is
      started.
    """

    def __init__(self, loop=None, *args, **kwargs):
        self._loop = loop or asyncio.get_event_loop()
        self._client = _Client(*args, **kwargs)

        self._reconnect_delay = None
        self._reconnect_min_delay = 1
        self._reconnect_max_delay = 120

        self._run = True

        self.waiting = {}  # list of events of messages waiting to be published

        self.misc_task = None
        self.disconnect_event = asyncio.Event()
        self.reconnect_task = None

        self._wrap_blocking_method("connect_srv")
        for register in self.internal_callbacks:
            register(self)

    async def wait_for_disconnect(self):
        await self.disconnect_event.wait()

    ###########################################################################
    # Utility functions for creating wrappers
    ###########################################################################

    def _wrap_blocking_method(self, name):
        """Wrap a blocking function to make it async."""
        f = getattr(self._client, name)

        @functools.wraps(f)
        async def wrapper(*args, **kwargs):
            return await self._loop.run_in_executor(
                None, functools.partial(f, *args, **kwargs))
        setattr(self, name, wrapper)

    def __getattr__(self, name):
        """Fall back on non-wrapped versions of most functions."""
        return getattr(self._client, name)

    ###########################################################################
    # Special-case wrappers around certain methods
    ###########################################################################

    @functools.wraps(_Client.publish)
    def publish(self, *args, **kwargs):
        # Produce an alternative MQTTMessageInfo object with a coroutine
        # wait_for_publish.
        return MQTTMessageInfo(
            self, self._client.publish(*args, **kwargs))

    @functools.wraps(_Client.message_callback_add)
    def message_callback_add(self, sub, callback):
        # Ensure callbacks are called from MQTT
        @functools.wraps(callback)
        def wrapper(_client, *args, **kwargs):
            self._loop.call_soon_threadsafe(
                functools.partial(callback, self, *args, **kwargs))
        self._client.message_callback_add(sub, wrapper)

    @functools.wraps(_Client.connect)
    async def connect(self, host, port=1883, keepalive=60, bind_address="",
                      retry=False):
        # reconnect for initial connection is immediately awaited
        # This will easily propogate connection exceptions
        self._client.connect_async(host, port, keepalive, bind_address)
        return await self.reconnect()

    @functools.wraps(_Client.connect_async)
    def connect_async(self, host, port=1883, keepalive=60, bind_address="",
                      retry=False):
        # reconnect for initial connection is run in the background
        # Connection exceptions will not be reraised unless the reconnect_task
        # is checked or awaited
        self._client.connect_async(host, port, keepalive, bind_address)
        self.reconnect_task = self._loop.create_task(self.reconnect(retry))
        self._run = True
        self.disconnect_event = asyncio.Event()
        return self.reconnect_task

    @functools.wraps(_Client.reconnect)
    async def reconnect(self, retry=False):
        # Runs reconnect in a separate thread.
        logging.debug("Reconnect")
        while self._run:
            try:
                return await self._loop.run_in_executor(
                    None, self._client.reconnect)
            except (socket.error, OSError, WebsocketConnectionError):
                if not retry:
                    raise
            if self._run:
                await self._reconnect_wait()

    # @functools.wraps(_Client.disconnect)
    # def disconnect(self):
    #     return self._client.disconnect()

    @functools.wraps(_Client.reconnect_delay_set)
    def reconnect_delay_set(self, min_delay=0, max_delay=120):
        # Reimplement reconnection logic from paho
        self._reconnect_min_delay = min_delay
        self._reconnect_max_delay = max_delay

    async def _reconnect_wait(self):
        # reimplement reconnection logic from paho
        if self._reconnect_delay is None:
            self._reconnect_delay = self._reconnect_min_delay
        else:
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                self._reconnect_max_delay
            )
        await asyncio.sleep(self._reconnect_delay)

    def _socket_read_ssl(self):
        # Call loop_read when socket is ready. Handle ssl weirdness
        while self._client.socket().pending():
            rc = self._client.loop_read()
            if rc or self._client.socket() is None:
                break

    def _socket_read(self):
        # Call loop_read when socket is ready
        self._client.loop_read()

    def _socket_write(self):
        # call loop_write when socket is ready
        self._client.loop_write()

    async def misc_loop(self):
        # call loop_misc on a regular basis
        while True:
            try:
                await asyncio.sleep(1)
            except CancelledError:
                logging.info("misc_loop Sleep cancelled")
                break
            rc = self._client.loop_misc()
            if rc or self._client.socket() is None:
                logging.info("misc_loop returned %d", rc)
                break

    # keep track of internal callbacks on register them in init
    internal_callbacks: List[Any] = []

    @internal_callback(internal_callbacks)
    def on_socket_register_write(self, userdata, sock):
        # Set callback when socket is ready for write
        self._loop.add_writer(sock, self._socket_write)

    @internal_callback(internal_callbacks)
    def on_socket_unregister_write(self, userdata, sock):
        # unset callback
        self._loop.remove_writer(sock)

    @internal_callback(internal_callbacks)
    def on_socket_open(self, userdata, sock):
        # Setup callbacks for socket reading
        if hasattr(sock, "pending"):
            self._loop.add_reader(sock, self._socket_read_ssl)
        else:
            self._loop.add_reader(sock, self._socket_read)

    @internal_callback(internal_callbacks)
    def on_socket_close(self, userdata, sock):
        # cleanup on socket close
        self._loop.remove_reader(sock)
        self.misc_task.cancel()

    @internal_callback(internal_callbacks)
    def on_disconnect(self, userdata, rc):
        # reconnect on error
        if rc != MQTT_ERR_SUCCESS:
            logging.info("Disconnect on error: %d", rc)
            if self.reconnect_task and self.reconnect_task.done():
                logging.info("Reconnecting on disconnect")
                self.reconnect_task = self._loop.create_task(
                    self.reconnect(True))
        else:
            self._run = False
            if self.reconnect_task and not self.reconnect_task.done():
                self.reconnect_task.cancel()
            self.disconnect_event.set()

    @internal_callback(internal_callbacks)
    def on_connect(self, userdata, flags_dict, result):
        # Make sure misc_loop is started, and reconnect_delay is reset
        self._reconnect_delay = None
        if not self.misc_task or self.misc_task.done():
            self.misc_task = self._loop.create_task(self.misc_loop())

    @internal_callback(internal_callbacks)
    def on_publish(self, userdata, mid):
        # Signal if any message is awaiting publication
        if mid in self.waiting:
            self.waiting[mid].set()

    @wrap_callback
    def on_message(self):
        pass

    @wrap_callback
    def on_subscribe(self):
        pass

    @wrap_callback
    def on_unsubscribe(self):
        pass

    @wrap_callback
    def on_log(self):
        pass
