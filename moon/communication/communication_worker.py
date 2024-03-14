from socket import socket
import time
import json
from http.server import BaseHTTPRequestHandler
from io import BytesIO
from logger import log
from threading import Thread, Lock
from communication.request import Request
from mapper.sql_analyzer import QueryAnalyzer
from communication.request_wrapper import RequestWrapper
from configuration.config import Configuration
from communication.new_request import NewRequest
from scheduler.scheduler import Scheduler


class HTTPRequest(BaseHTTPRequestHandler):
    def __init__(self, request_text):
        self.rfile = BytesIO(request_text.encode('utf-8'))
        self.raw_requestline = self.rfile.readline()
        self.error_code = self.error_message = None
        self.parse_request()

    def send_error(self, code, message):
        self.error_code = code
        self.error_message = message


class Worker(Thread):
    id_workers = 0
    mutex = Lock()

    def __init__(self, con: socket, client, scheduler: Scheduler):
        Thread.__init__(self)

        self.id = self.get_id_worker()
        self.con = con
        self.client = client
        self.scheduler = scheduler

        log.i(
            f"Communication Module - Worker {self.id}",
            'Start ...'
        )

    @staticmethod
    def get_id_worker():
        """
        Returns the next worker id
        Mutex is used to guarantee that each worker has a unique id
        """
        Worker.mutex.acquire()
        Worker.id_workers += 1
        Worker.mutex.release()
        return Worker.id_workers

    def handle_request(self, request_body):
        log.i(
            f"Communication Module - Worker {self.id}",
            f"Request received: {request_body}"
        )

        # Creating the request object from message body
        request_body = NewRequest(request_body)

        # Putting it in a wrapper
        request_wrapper = RequestWrapper(request_body)

        # Sending to the scheduler
        self.scheduler.enqueue_transaction(request_wrapper)

        # Waiting for response ready
        request_wrapper.ready.wait()

        # Getting the response from wrapper
        response_body = str(request_wrapper.result)

        response = f"HTTP/1.1 200 OK\nContent-Length: {len(response_body)}\r\n\r\n{response_body}"

        return response

    def run(self):
        """
        Receives transactions from the clients
        and enqueue it using the scheduler module
        """

        log.i(
            f"Communication Module - Worker {self.id}",
            f"New Connection: {self.client}"
        )

        request_text = self.con.recv(1024).strip().decode('utf-8')
        request = HTTPRequest(request_text)

        if request.error_code:
            log.e(
                f"Communication Module - Worker {self.id}",
                f"Error: {request.error_code} {request.error_message}"
            )
            return

        if not request.headers['Content-Length']:
            log.e(
                f"Communication Module - Worker {self.id}",
                'Error: Content-Length not found'
            )
            return

        body = request.rfile.read(
            int(request.headers['Content-Length'])).decode('utf-8')
        response = self.handle_request(body)

        self.con.send(response.encode('utf-8'))

        log.i(
            f"Communication Module - Worker {self.id}",
            f"Response sent: {response}"
        )

        # The client sent END_COMMUNICATION
        self.con.close()
        log.i(
            f"Communication Module - Worker {self.id}",
            'Connection Closed'
        )
