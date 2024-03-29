import socket


class MoonClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def send_query(self, query):
        """
        Sends a Request to MOON
        :param query: The MOON query
        :return: The response query
        """
        request = query
        response = None

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(request.encode('utf-8'))
            response = s.recv(1024)
            s.close()

        return response
