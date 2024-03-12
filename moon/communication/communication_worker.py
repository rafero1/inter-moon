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
            f'Communication Module - Worker {self.id}',
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
            f'Communication Module - Worker {self.id}',
            f'New Connection: {self.client}'
        )

        # Waiting requests from this connection
        msg = self.con.recv(1024)
        msg = msg.decode('utf-8')

        log.i(
            f'Communication Module - Worker {self.id}',
            f'Request Received: {msg}'
        )

        # Split the request by double newline characters
        split_request = msg.split('\r\n\r\n')

        # The body of the request is the second part
        body = split_request[1] if len(split_request) > 1 else ''
        body = " ".join(body.split()) # normalize spaces

        log.i(
            f'Communication Module - Worker {self.id}',
            f'Request Body: {body}'
        )

        # Creating the request object from message body
        request = NewRequest(body)

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
            f'Communication Module - Worker {self.id}',
            'Response sent: {}'.format(response)
        )

        # The client sent END_COMMUNICATION
        self.con.close()
        log.i(
            f'Communication Module - Worker {self.id}',
            'Connection Closed'
        )
