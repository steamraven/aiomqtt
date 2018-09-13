"""
This test suite functions more as a sanity check than a comprehensive test.
"""

import asyncio
import logging

import pytest
from mock import Mock, patch

import aiomqtt
import socket as _socket


@pytest.fixture("module")
def port():
    # A port which is likely to be free for the duration of tests...
    return 11223


@pytest.fixture("module")
def hostname():
    return "localhost"


@pytest.fixture("module")
def event_loop():
    return asyncio.get_event_loop()


@pytest.yield_fixture(scope="module")
def server(event_loop, port):
    mosquitto = event_loop.run_until_complete(asyncio.create_subprocess_exec(
        "mosquitto", "-p", str(port),
        stdout=asyncio.subprocess.DEVNULL,
        # stderr=asyncio.subprocess.DEVNULL,
        loop=event_loop))

    try:
        yield
    finally:
        mosquitto.terminate()
        event_loop.run_until_complete(mosquitto.wait())


def test_native_client(server, hostname, port):
    """Sanity check: Make sure the paho-mqtt client can connect to the test
    MQTT server.
    """

    import paho.mqtt.client as mqtt
    import threading

    c = mqtt.Client()
    c.loop_start()
    try:
        # Just make sure the client connects successfully
        on_connect = threading.Event()
        c.on_connect = Mock(side_effect=lambda *_: on_connect.set())
        c.connect_async(hostname, port)
        assert on_connect.wait(5)
    finally:
        c.loop_stop()


@pytest.mark.asyncio
async def test_connect_and_loop_forever(server, hostname, port, event_loop):
    """Tests connecting and then disconnecting from the MQTT server while using
    the loop_forever construct.
    """

    c = aiomqtt.Client(loop=event_loop)

    # Immediately disconnect on connection
    connect_event = asyncio.Event(loop=event_loop)

    def on_connect(client, userdata, flags, rc):
        assert client is c
        assert userdata is None
        assert isinstance(flags, dict)
        assert rc == aiomqtt.MQTT_ERR_SUCCESS

        connect_event.set()
        c.disconnect()
    c.on_connect = Mock(side_effect=on_connect)

    # Just check disconnect event is as expected
    disconnect_event = asyncio.Event(loop=event_loop)

    def on_disconnect(client, userdata, rc):
        assert client is c
        assert userdata is None
        assert rc == aiomqtt.MQTT_ERR_SUCCESS

        disconnect_event.set()
    c.on_disconnect = Mock(side_effect=on_disconnect)

    # When the client disconnects, this call should end
    c.connect_async(hostname, port)
    await asyncio.wait_for(disconnect_event.wait(), timeout=5, loop=event_loop)

    # Should definately have connected and disconnected
    assert connect_event.is_set()
    assert c.on_connect.called
    assert disconnect_event.is_set()
    assert c.on_disconnect.called


@pytest.mark.asyncio
@patch('socket.create_connection')
async def test_reconnect(create_connection, server, hostname, port, event_loop):
    """Tests connecting and then disconnecting from the MQTT server while using
    the loop_forever construct.
    """
    create_connection.side_effect = _socket.create_connection
    c = aiomqtt.Client(loop=event_loop)
    c.enable_logger()

    # Immediately disconnect on connection
    connect_event = asyncio.Event(loop=event_loop)

    num_called = 0

    def on_connect(client, userdata, flags, rc):
        assert client is c
        assert userdata is None
        assert isinstance(flags, dict)
        assert rc == aiomqtt.MQTT_ERR_SUCCESS

        connect_event.set()
        nonlocal num_called
        num_called += 1
        if num_called == 1:
            create_connection.return_value.close()
        elif num_called == 2:
            c.disconnect()
        else:
            assert False, "Called more than twice"
    c.on_connect = Mock(side_effect=on_connect)

    # Just check disconnect event is as expected
    disconnect_event = asyncio.Event(loop=event_loop)

    def on_disconnect1(client, userdata, rc):
        assert client is c
        assert userdata is None

        if rc == aiomqtt.MQTT_ERR_SUCCESS:
            disconnect_event.set()
    c.on_disconnect = Mock(side_effect=on_disconnect1)

    # When the client disconnects, this call should end
    c.connect_async(hostname, port)
    await asyncio.wait_for(disconnect_event.wait(), timeout=5, loop=event_loop)

    # Should definately have connected and disconnected
    assert connect_event.is_set()
    assert len(c.on_connect.mock_call) == 2
    assert disconnect_event.is_set()
    assert len(c.on_disconnect.mock_call) == 2


@pytest.mark.asyncio
async def test_pub_sub(server, hostname, port, event_loop):
    """Make sure the full set of publish and subscribe functions and callbacks
    work.
    """
    c = aiomqtt.Client(loop=event_loop)

    c.enable_logger()
    logging.basicConfig(level=logging.DEBUG)

    connect_event = asyncio.Event(loop=event_loop)
    c.on_connect = Mock(side_effect=lambda *_: connect_event.set())

    subscribe_event = asyncio.Event(loop=event_loop)
    c.on_subscribe = Mock(side_effect=lambda *_: subscribe_event.set())

    publish_event = asyncio.Event(loop=event_loop)
    c.on_publish = Mock(side_effect=lambda *_: publish_event.set())

    message_event = asyncio.Event(loop=event_loop)
    c.on_message = Mock(side_effect=lambda *_: message_event.set())

    # For message_callback_add
    message_callback_event = asyncio.Event(loop=event_loop)
    message_callback = Mock(
        side_effect=lambda *_: message_callback_event.set())

    unsubscribe_event = asyncio.Event(loop=event_loop)
    c.on_unsubscribe = Mock(side_effect=lambda *_: unsubscribe_event.set())

    disconnect_event = asyncio.Event(loop=event_loop)
    c.on_disconnect = Mock(side_effect=lambda *_: disconnect_event.set())

    try:
        c.connect_async(hostname, port)
        await asyncio.wait_for(
            connect_event.wait(), timeout=5, loop=event_loop)

        # Test subscription
        result, mid = c.subscribe("test")
        assert result == aiomqtt.MQTT_ERR_SUCCESS
        assert mid is not None
        await asyncio.wait_for(
            subscribe_event.wait(), timeout=5, loop=event_loop)
        c.on_subscribe.assert_called_once_with(c, None, mid, (0,))

        # Test publishing
        message_info = c.publish("test", "Hello, world!")
        result, mid = message_info
        assert result == aiomqtt.MQTT_ERR_SUCCESS
        assert mid is not None
        await asyncio.wait_for(
            message_info.wait_for_publish(), timeout=5, loop=event_loop)
        c.on_publish.assert_called_once_with(c, None, mid)

        # Test message arrives
        await asyncio.wait_for(
            message_event.wait(), timeout=5, loop=event_loop)
        assert len(c.on_message.mock_calls) == 1
        assert c.on_message.mock_calls[0][1][0] is c
        assert c.on_message.mock_calls[0][1][1] is None
        assert c.on_message.mock_calls[0][1][2].topic == "test"
        assert c.on_message.mock_calls[0][1][2].payload == b"Hello, world!"

        # Now test with alternative message callback
        c.message_callback_add("test", message_callback)

        # Send another message
        message_info = c.publish("test", "Hello, again!")

        # Test message arrives
        await asyncio.wait_for(
            message_callback_event.wait(), timeout=5, loop=event_loop)
        assert len(message_callback.mock_calls) == 1
        assert message_callback.mock_calls[0][1][0] is c
        assert message_callback.mock_calls[0][1][1] is None
        assert message_callback.mock_calls[0][1][2].topic == "test"
        assert message_callback.mock_calls[0][1][2].payload == b"Hello, again!"

        # Test un-subscription
        result, mid = c.unsubscribe("test")
        assert result == aiomqtt.MQTT_ERR_SUCCESS
        assert mid is not None
        await asyncio.wait_for(
            unsubscribe_event.wait(), timeout=5, loop=event_loop)
        c.on_unsubscribe.assert_called_once_with(c, None, mid)
        c.disconnect()
        await asyncio.wait_for(disconnect_event.wait(), timeout=5,
                               loop=event_loop)
    finally:
        pass

    assert len(c.on_connect.mock_calls) == 1
    assert len(c.on_subscribe.mock_calls) == 1
    assert len(c.on_publish.mock_calls) == 2
    assert len(c.on_message.mock_calls) == 1
    assert len(message_callback.mock_calls) == 1
    assert len(c.on_unsubscribe.mock_calls) == 1
    assert len(c.on_disconnect.mock_calls) == 1
