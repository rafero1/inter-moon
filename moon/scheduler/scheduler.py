import sys
import queue
from logger import log
from threading import Thread
from mapper.sql_analyzer import QueryAnalyzer
from sqlclient.clientsql import ClientSQL
from bcclient.bcclient import ClientBlockchain
from mapper.schema_manager import SchemaManager
from mapper.persistence_model import BLOCKCHAIN
from communication.query_type import QueryType
from configuration.config import Configuration

class Scheduler(Thread):
    queue = queue.Queue()
    config = Configuration.get_instance()

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

                # identify request type
                request.q_type = QueryAnalyzer.get_type_query(request.q_query)

                log.i(
                    'Scheduler Module',
                    f"Request type: {QueryType(request.q_type).name}"
                )

                # identify entities from the request
                request.q_entities = QueryAnalyzer.get_entities(request.q_query)

                is_bc = False

                try:
                    # check if any of the entities is stored in the blockchain
                    for entity in request.q_entities:
                        entity_type = SchemaManager.get_entity_db(entity)
                        if entity_type == BLOCKCHAIN:
                            is_bc = True
                            break
                    if not is_bc:
                        cli = ClientSQL(request)
                    else:
                        cli = ClientBlockchain(request)

                    # TODO: proper multi-threading (switch to multiprocessing?)
                    # consider using asyncio
                    cli.start()
                    cli.join()

                    request_wrapper.result = cli.get_result()
                    request_wrapper.ready.set()
                except Exception as e:
                    log.e(
                        'Scheduler Module',
                        sys.exc_info()
                    )

                    request_wrapper.result = str(e)
                    request_wrapper.ready.set()
