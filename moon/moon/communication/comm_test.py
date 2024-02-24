import socket
from moon.communication.request import Request


for i in range(10):
    a = Request("select * from..", "postgres", "db_name",
                "ufc123", "127.0.0.1", "9863", "bc_public_key",
                "bc_private_key", "select", [1, 2, 23])
    HOST = '127.0.0.1'  # Endereco IP do Servidor
    PORT = 9654  # Porta que o Servidor esta
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    dest = (HOST, PORT)
    tcp.connect(dest)
    tcp.send(str(a).encode("utf-8"))
    msg_rcv = tcp.recv(1024)
    print("received:", msg_rcv)
# tcp.send("END_COMMUNICATION".encode("utf-8"))
# tcp.close()
