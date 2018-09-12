import asyncio
import functools
from asyncio import (CancelledError, iscoroutinefunction,
                     run_coroutine_threadsafe)

from paho.mqtt.client import MQTT_ERR_SUCCESS
from paho.mqtt.client import Client as _Client


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
                run_coroutine_threadsafe(value(self, *args), self._loop)
        else:
            def wrapper(_client, *args):
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
                    self._loop.call_soon_threadsafe(func, self, *args)
                setattr(self._client, func.__name__, wrapper)
            else:
                def wrapper2(_client, *args):
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
        for register in self.internal_callbacks:
            register(self)

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
        self.reconnect_task = self._loop.create_task(self.reconnect())
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

    internal_callbacks = []

    @internal_callback(internal_callbacks)
    def on_socket_register_write(self, userdata, sock):
        self._loop.add_writer(sock, self._socket_write)

    @internal_callback(internal_callbacks)
    def on_socket_unregister_write(self, userdata, sock):
        self._loop.remove_writer(sock)

    @internal_callback(internal_callbacks)
    def on_socket_open(self, userdata, sock):
        if hasattr(sock, "pending"):
            self._loop.add_reader(sock, self._socket_read_ssl)
        else:
            self._loop.add_reader(sock, self._socket_read)
        if self.misc_task:
            self.misc_task.cancel()
        self.misc_task = self._loop.create_task(self.misc_loop())

    @internal_callback(internal_callbacks)
    def on_socket_close(self, userdata, sock):
        self._loop.remove_reader(sock)
        self.misc_task.cancel()

    @internal_callback(internal_callbacks)
    def on_disconnect(self, userdata, rc):
        if rc != MQTT_ERR_SUCCESS:
            assert self.reconnect_task.done()
            self.reconnect_task = self._loop.create_task(self.reconnect())

    @wrap_callback
    def on_connect(self):
        pass

    @wrap_callback
    def on_message(self):
        pass

    @wrap_callback
    def on_publish(self):
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

    async def misc_loop(self):
        while True:
            try:
                await asyncio.sleep(1)
            except CancelledError:
                break
            rc = self._client.loop_misc()
            if rc or self._client.socket() is None:
                break
