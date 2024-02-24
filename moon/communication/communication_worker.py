from socket import socket
import time
import json
from logger import log
from threading import Thread, Lock
from communication.request import Request
from mapper.sql_analyzer import QueryAnalyzer
from communication.request_wrapper import RequestWrapper
from configuration.config import Configuration
from communication.new_request import NewRequest
from scheduler.scheduler import Scheduler


class Worker(Thread):
    id_workers = 0
    mutex = Lock()

    def __init__(self, con: socket, client, scheduler: Scheduler):
        super().__init__()

        self.id = self.get_id_worker()
        self.con = con
        self.client = client
        self.scheduler = scheduler

        log.i(
            'Communication Module - Worker {}'.format(self.id),
            'Start ...'
        )

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

        # Waiting requests from this connection
        msg = self.con.recv(1024)
        msg = msg.decode('utf-8')
        msg = " ".join(msg.split()) # normalize spaces

        log.i(
            'Communication Module - Worker {}'.format(self.id),
            'Request Received: {}'.format(msg)
        )

        # Creating the request object from string message
        # request_dict = json.loads(msg)
        request = NewRequest(msg)

        # Putting it in a wrapper
        request_wrapper = RequestWrapper(request)

        # Sending to the scheduler
        self.scheduler.enqueue_transaction(request_wrapper)

        # Waiting for response ready
        request_wrapper.ready.wait()

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
