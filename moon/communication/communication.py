import sys
import socket
from logger import log
from scheduler.scheduler import Scheduler
from communication.communication_worker import Worker


class Communication:
    def __init__(self, host, port):
        try:
            self.host = host
            self.port = port

            scheduler = Scheduler()
            scheduler.start()

            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.bind((self.host, self.port))
            tcp.listen(1)
            log.i('Communication Module', 'Server Started on {}:{}'.format(self.host, self.port))

            while True:
                con, client = tcp.accept()
                worker = Worker(con, client, scheduler)
                worker.start()

        except Exception as e:
            log.e('Communication Module', sys.exc_info())
