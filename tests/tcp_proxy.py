import asyncio
import logging


class TcpProxy:
    def __init__(self, local_port, hostname, remote_port, loop):
        self.local_port = local_port
        self.hostname = hostname
        self.remote_port = remote_port
        self.loop = loop
        self.discard = False

    async def connect(self):
        logging.debug("Connecting proxy")
        self.stop = asyncio.Event()
        self.server = await asyncio.start_server(self.handle_client,
                                                 '127.0.0.1',
                                                 self.local_port,
                                                 loop=self.loop)

    async def close(self):
        logging.debug("Closing proxy")
        self.server.close()
        self.stop.set()
        await self.server.wait_closed()

    async def interrupt(self):
        await self.close()
        await self.connect()

    def start_discard(self):
        self.discard = True

    def stop_discard(self):
        self.discard = False

    async def pipe(self, reader, writer):
        try:
            while not reader.at_eof():
                data = await reader.read(2048)
                if not self.discard:
                    writer.write(data)
                    await writer.drain()
        finally:
            writer.close()

    async def handle_client(self, local_reader, local_writer):
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                self.hostname, self.remote_port, loop=self.loop)

            pipe1 = self.pipe(local_reader, remote_writer)
            pipe2 = self.pipe(remote_reader, local_writer)
            _, pending = await asyncio.wait(
                (
                    self.stop.wait(),
                    asyncio.gather(pipe1, pipe2)
                ),
                return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
            remote_writer.close()
        except OSError:
            logging.error("Cannot connect to remote: %s:%d",
                          self.hostname, self.remote_port)
        finally:
            local_writer.close()
