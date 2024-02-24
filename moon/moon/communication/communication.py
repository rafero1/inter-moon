import sys
import socket
from moon.logger import log
from moon.scheduler.scheduler import Scheduler
from moon.communication.communication_worker import Worker


class Communication:
    def __init__(self, host, port):
        try:
            scheduler = Scheduler()
            scheduler.start()
            self.host = host
            self.port = port
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            orig = (self.host, self.port)
            tcp.bind(orig)
            tcp.listen(1)
            log.i('Communication Module', 'Server Started on {}:{}'.format(self.host, self.port))
            while True:
                con, client = tcp.accept()
                worker = Worker(con, client, scheduler)
                worker.start()
        except Exception as e:
            log.e('Communication Module', sys.exc_info())


# if __name__ == "__main__":
#     a = Communication("", 9654)
