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


class Worker():
    id_workers = 0
    mutex = Lock()

    def __init__(self, data, scheduler: Scheduler):
        self.id = self.get_id_worker()
        self.data = data
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

    async def handle_request(self, request_body):
        log.i(
            f"Communication Module - Worker {self.id}",
            f"Request received: {request_body}"
        )

        # Creating the request object from message body
        request_body = NewRequest(request_body)

        # Putting it in a wrapper
        request_wrapper = RequestWrapper(request_body)

        # Sending to the scheduler
        await self.scheduler.enqueue_transaction(request_wrapper)

        # Waiting for response ready
        await request_wrapper.ready.wait()

        # Getting the response from wrapper
        response_body = str(request_wrapper.result)

        response = f"HTTP/1.1 200 OK\nContent-Length: {len(response_body)}\r\n\r\n{response_body}"

        return response

    async def start(self):
        """
        Receives transactions from the clients
        and enqueue it using the scheduler module
        """

        request_text = self.data.decode('utf-8')
        request = HTTPRequest(request_text)

        if request.error_code:
            log.e(
                f"Communication Module - Worker {self.id}",
                f"Error: {request.error_code} {request.error_message}"
            )
            return "HTTP/1.1 400 Bad Request\r\n\r\n" + str(request.error_message)

        if not request.headers['Content-Length']:
            log.e(
                f"Communication Module - Worker {self.id}",
                'Error: Content-Length not found'
            )
            return "HTTP/1.1 400 Bad Request\r\n\r\nMissing Content-Length header"

        body = request.rfile.read(
            int(request.headers['Content-Length'])).decode('utf-8')
        response = await self.handle_request(body)

        return response
