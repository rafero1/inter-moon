import sys
import queue
from moon.logger import log
from threading import Thread
from moon.sqlclient.clientsql import ClientSQL
from moon.bcclient.bcclient import ClientBlockchain
from moon.mapper.schema_manager import SchemaManager
from moon.mapper.persistence_model import BLOCKCHAIN


class Scheduler(Thread):
    queue = queue.Queue()

    @staticmethod
    def enqueue_transaction(request_wrapper):
        """
        Queues a request. This queue implementation
        already handles concurrency control
        """
        Scheduler.queue.put(request_wrapper)

    def run(self):
        while True:
            if Scheduler.queue.empty() is False:
                request_wrapper = Scheduler.queue.get(block=True)
                request = request_wrapper.request
                bc = False
                try:
                    for entity in request.q_entities:
                        if (
                            SchemaManager.get_entity_persistence(entity)
                            == BLOCKCHAIN
                        ):
                            bc = True
                            break
                    if not bc:
                        cli = ClientSQL(request)
                        cli.start()
                        cli.join()
                    else:
                        cli = ClientBlockchain(request)
                        cli.start()
                        cli.join()
                    request_wrapper.result = cli.get_result()
                    request_wrapper.ready = True
                except Exception as e:
                    log.e(
                        'Scheduler Module',
                        sys.exc_info()
                    )
                    request_wrapper.result = str(e)
                    request_wrapper.ready = True
