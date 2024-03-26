import os
from dotenv import load_dotenv
from communication.communication import Communication
import asyncio
from logger import log
from scheduler.scheduler import Scheduler
from communication.communication_worker import Worker

load_dotenv()

scheduler = Scheduler()

async def main():
    comm = Communication(os.environ['MOON_HOST'], int(os.environ['MOON_PORT']))
    await comm.start()

asyncio.run(main())
