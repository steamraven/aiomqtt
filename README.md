`aiomqtt`: An asyncio Wrapper for paho-mqtt
===========================================

This library implements a minimal Python 3
[asyncio](https://docs.python.org/3/library/asyncio.html) wrapper around the
MQTT client in [paho-mqtt](https://github.com/eclipse/paho.mqtt.python).

Installation:

    pip install aiomqtt

API
---

This library is as thin as possible, exposing the exact same API as the
original paho-mqtt `Client` object with blocking calls replaced with coroutines
and all callbacks being scheduled into the asyncio main event loop. It does not
attempt to introduce a more idiomatic asyncio API.

When using aiomqtt, refer to the [paho-mqtt
documentation](https://pypi.python.org/pypi/paho-mqtt/1.1) which applies
verbatim with the exception of the above rules. An example use of the library
is shown below:

    import asyncio
    import aiomqtt
    
    loop = asyncio.get_event_loop()
    
    async def demo():
        c = aiomqtt.Client(loop)
        
        connected = asyncio.Event(loop=loop)
        await c.connect("localhost")
        print("Connected!")
        

        async def on_message(client, userdata, message):
            print("Got message:", message.topic, message.payload)

        c.message_callback_add("my/test/path", on_message)

        c.subscribe("my/test/path")
        print("Subscribed to my/test/path")
                
        message_info = c.publish("my/test/path", "Hello, world")
        await message_info.wait_for_publish()
        print("Message published!")
        
        await asyncio.sleep(1, loop=loop)
        print("Disconnecting...")
        
        disconnected = asyncio.Event(loop=loop)
        def on_disconnect(client, userdata, rc):
            disconnected.set()
        c.on_disconnect = on_disconnect
        c.disconnect()
        await disconnected.wait()
        print("Disconnected")
        
    
    loop.run_until_complete(demo())


About that loop...
------------------

Unfortunately the author was unable to work out how to integrate paho-mqtt's
event loop into asyncio, despite the best efforts of the paho-mqtt authors to
make this possible. (Patches are welcome.)

Instead, `loop_start()` and `loop_stop()` may be used as normal (and aiomqtt
will ensure callbacks arrive in the correct thread) or `loop_forever()` may be
used which in aiomqtt is a coroutine.
