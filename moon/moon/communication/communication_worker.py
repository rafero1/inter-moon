import time
import json
from moon.logger import log
from threading import Thread, Lock
from moon.communication.request import Request
from moon.mapper.sql_analyzer import QueryAnalyzer
from moon.communication.request_wrapper import RequestWrapper


class Worker(Thread):
    id_workers = 0
    mutex = Lock()

    def __init__(self, con, client, scheduler):
        super().__init__()
        self.id = self.get_id_worker()
        log.i(
            'Communication Module - Worker {}'.format(self.id),
            'Start ...'
        )
        self.con = con
        self.client = client
        self.scheduler = scheduler

    @staticmethod
    def get_id_worker():
        """
        Getting the id to the thread.
        It is atomic using mutex
        :return: id to the thread
        """
        Worker.mutex.acquire()
        Worker.id_workers += 1
        Worker.mutex.release()
        return Worker.id_workers

    def run(self):
        """
        Receives transactions from the clients
        and enqueue it using the scheduler module
        """
        log.i(
            'Communication Module - Worker {}'.format(self.id),
            'New Connection: {}'.format(self.client)
        )
        while True:
            # Waiting requests from this connection
            msg = self.con.recv(1024)
            msg = msg.decode('utf-8')

            # Close the connection
            if msg == 'END_COMMUNICATION':
                break
            log.i(
                'Communication Module - Worker {}'.format(self.id),
                'Request Received: {}'.format(msg)
            )

            # Creating the request object from string message
            request_dict = json.loads(msg)

            # Request object
            request = Request.make_by_dict(request_dict)

            # Getting type and entities from query
            query_type = QueryAnalyzer.get_type_query(
                query=request.q_query
            )
            entities = QueryAnalyzer.get_involved_entities(
                type_query=query_type,
                query=request.q_query
            )

            request.q_type = query_type
            request.q_entities = entities

            # Putting it in a wrapper
            request_wrapper = RequestWrapper(request)

            # Sending to the scheduler
            self.scheduler.enqueue_transaction(request_wrapper)

            # Waiting for response ready
            while True:
                if request_wrapper.ready:
                    break
                # time.sleep(1)

            # Getting the response from wrapper
            response = str(request_wrapper.result)

            # Sending it back to the client
            self.con.send(response.encode('utf-8'))
            log.i(
                'Communication Module - Worker {}'.format(self.id),
                'Response sent: {}'.format(response)
            )

        # The client sent END_COMMUNICATION
        self.con.close()
        log.i(
            'Communication Module - Worker {}'.format(self.id),
            'Connection Closed'
        )
