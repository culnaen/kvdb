import logging
import socket
import threading

from .storage import Storage
from .connection import Connection


logger = logging.getLogger(__name__)


class KVDB:
    def __init__(self):
        self._lock = threading.RLock()
        self._storage = Storage(self._lock)

    def connect(self):
        return Connection(self._storage)

    @property
    def storage(self):
        return self._storage.data

    def start_socket_server(self, host, port):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((host, port))
        server.listen(5)
        while True:
            client, address = server.accept()
            logger.info(f"Connected to :{address[0]}:{address[1]}")
            db_connection = self.connect()
            connection = threading.Thread(target=self._socket_connection, args=(client, db_connection))
            connection.start()

    def _socket_connection(self, client, db_connection):
        msg = []
        while True:
            data = client.recv(1024)
            if not data:
                break
            msg.append(data)
            if data == b"\r\n":
                cmd = b"".join(msg).decode()[:-2]
                logger.info(cmd)
                with self._lock:
                    result = db_connection.execute(cmd)
                logger.info(result)
                client.send(str(result).encode() + b"\r\n")
                msg.clear()
        try:
            self._lock.release()
        except RuntimeError:
            logger.info("release un-acquired lock")
        db_connection.close()
        client.close()
        logger.info("close socket connection")
