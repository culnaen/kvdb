import time
from collections import deque

from .storage import Command, Storage
from .type import DequeTransactions


class Connection:
    def __init__(self, storage: Storage):
        self._storage = storage
        self._local_transactions: DequeTransactions = deque()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    def execute(self, data: str):
        cmd, args = self.parse_data(data)
        return self._storage.commands[cmd](self._local_transactions, *args)

    def parse_data(self, data: str):
        cmd_name, *args = data.split()
        cmd = Command(cmd_name)
        return cmd, args

    def close(self):
        time.sleep(0.1)

    @property
    def transactions(self):
        return self._local_transactions
