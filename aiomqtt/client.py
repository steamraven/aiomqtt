import asyncio
import functools
from asyncio import (CancelledError, iscoroutinefunction,
                     run_coroutine_threadsafe)

from paho.mqtt.client import MQTT_ERR_SUCCESS
from paho.mqtt.client import Client as _Client


class MQTTMessageInfo(object):

    def __init__(self, loop, mqtt_message_info):
        self._loop = loop
        self._mqtt_message_info = mqtt_message_info

    def __getattr__(self, name):
        return getattr(self._mqtt_message_info, name)

    async def wait_for_publish(self):
        return await self._loop.run_in_executor(
            None, self._mqtt_message_info.wait_for_publish)

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

        self.misc_task = None

        self._wrap_blocking_method("connect")
        self._wrap_blocking_method("connect_srv")
        self._wrap_blocking_method("reconnect")

        self._wrap_blocking_method("loop_forever")
        self._wrap_blocking_method("loop_stop")

        self._wrap_callback("on_connect")
        self._wrap_callback("on_disconnect", self._on_disconnect)
        self._wrap_callback("on_message")
        self._wrap_callback("on_publish")
        self._wrap_callback("on_subscribe")
        self._wrap_callback("on_unsubscribe")
        self._wrap_callback("on_log")
        self._wrap_callback("on_socket_open", self._on_socket_open)
        self._wrap_callback("on_socket_close", self._on_socket_close)
        self._wrap_callback("on_socket_register_write",
                            self._on_socket_register_write)
        self._wrap_callback("on_socket_unregister_write",
                            self._on_socket_unregister_write)

    ###########################################################################
    # Utility functions for creating wrappers
    ###########################################################################

    def _wrap_callback(self, name, internal_callback=None):
        """Add the named callback to the MQTT client which triggers a call to
        the wrapper's registered callback in the event loop thread.
        """
        setattr(self, name, None)

        def wrapper(_client, *args):
            f = getattr(self, name)
            if f is not None:
                if iscoroutinefunction(f):
                    run_coroutine_threadsafe(f(self, *args), self._loop)

                else:
                    self._loop.call_soon_threadsafe(f, self, *args)

        if internal_callback:
            def wrapper2(_client, *args):
                self._loop.call_soon_threadsafe(internal_callback, *args)
                wrapper(_client, *args)
            setattr(self._client, name, wrapper2)
        else:
            setattr(self._client, name, wrapper)

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
            self._loop, self._client.publish(*args, **kwargs))

    @functools.wraps(_Client.message_callback_add)
    def message_callback_add(self, sub, callback):
        # Ensure callbacks are called from MQTT
        @functools.wraps(callback)
        def wrapper(_client, *args, **kwargs):
            self._loop.call_soon_threadsafe(
                functools.partial(callback, self, *args, **kwargs))
        self._client.message_callback_add(sub, wrapper)

    @functools.wraps(_Client.connect_async)
    def connect_async(self, host, port=1883, keepalive=60, bind_address=""):
        self._client.connect_async(host, port, keepalive, bind_address)
        self.reconnect_task =  self._loop.create_task(self.reconnect())
        return self.reconnect_task

    def _socket_read_ssl(self):
        while self._client.socket().pending():
            rc = self._client.loop_read()
            if rc or self._client.socket() is None:
                break

    def _socket_read(self):
        self._client.loop_read()
        # if rc or self._client.socket() is None:
        #    # reconnect
        #    pass

    def _socket_write(self):
        self._client.loop_write()
        # if rc or self._client.socket() is None:
        #    # reconnect
        #    pass

    def _on_socket_register_write(self, userdata, sock):
        self._loop.add_writer(sock, self._socket_write)

    def _on_socket_unregister_write(self, userdata, sock):
        self._loop.remove_writer(sock)

    def _on_socket_open(self, userdata, sock):
        if hasattr(sock, "pending"):
            self._loop.add_reader(sock, self._socket_read_ssl)
        else:
            self._loop.add_reader(sock, self._socket_read)
        if self.misc_task:
            self.misc_task.cancel()
        self.misc_task = self._loop.create_task(self._misc())

    def _on_socket_close(self, userdata, sock):
        self._loop.remove_reader(sock)
        self.misc_task.cancel()

    def _on_disconnect(self, userdata, rc):
        if rc != MQTT_ERR_SUCCESS:
            assert self.reconnect_task.done()
            self.reconnect_task = self._loop.create_task(self.reconnect())

    async def _misc(self):
        while True:
            try:
                await asyncio.sleep(1)
            except CancelledError:
                break
            rc = self._client.loop_misc()
            if rc or self._client.socket() is None:
                break
