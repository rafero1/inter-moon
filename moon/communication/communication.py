import sys
import socket
from logger import log
from scheduler.scheduler import Scheduler
from communication.communication_worker import Worker
import asyncio

class Communication:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.scheduler = Scheduler()

    async def start(self):
        try:
            log.i('Communication Module', 'Starting scheduler...')
            self.scheduler_task = asyncio.create_task(self.scheduler.start())

            log.i('Communication Module', f'Starting server on {self.host}:{self.port}...')
            server = await asyncio.start_server(self.handle_client, self.host, self.port)

            async with server:
                await server.serve_forever()
        except Exception as e:
            log.e('Communication Module', f'Error: {e}')

    async def handle_client(self, reader, writer):
        try:
            data = await reader.read(1000)
            log.i('Communication Module', f"Data received: {data}")

            worker = Worker(data, self.scheduler)
            response = await worker.start()

            response = response or ""
            log.i('Communication Module', f"Response: {response}")

            writer.write(response.encode())
            await writer.drain()

        except Exception as e:
            log.e('Communication Module', f'Error: {e}')

        finally:
            writer.close()
            await writer.wait_closed()
