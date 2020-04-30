from typing import Dict, Optional, Set
from threading import Lock
from enum import Enum

from .types import QueueTransactions


class Command(Enum):
    GET = "GET"
    SET = "SET"
    UNSET = "UNSET"
    COUNTS = "COUNTS"
    BEGIN = "BEGIN"
    ROLLBACK = "ROLLBACK"
    COMMIT = "COMMIT"
    END = "END"


class Storage:
    def __init__(self):
        self._data = {}
        self._lock = Lock()
        self._commands = {
            Command.GET: self.get,
            Command.SET: self.set,
            Command.UNSET: self.unset,
            Command.COUNTS: self.counts,
            Command.BEGIN: self.begin,
            Command.ROLLBACK: self.rollback,
            Command.COMMIT: self.commit,
        }

    def get(self, transactions: QueueTransactions, key: str):
        return self._get(transactions, key)

    def _get(self, transactions: QueueTransactions, key: str):
        if transactions:
            result = transactions[-1].get(key)
            if result is not None:
                return result
        return self._data.get(key)

    def set(self, transactions: QueueTransactions, key: str, value: str):
        if transactions:
            transactions[-1][key] = value
        else:
            self._data[key] = value

    def unset(self, transactions: QueueTransactions, key: str):
        if transactions:
            transactions[-1][key] = None
        else:
            self._data.pop(key, None)

    def begin(self, transactions: QueueTransactions):
        transactions.append(dict())

    def rollback(self, transactions: QueueTransactions):
        transactions.pop()

    def commit(self, transactions: QueueTransactions):
        with self._lock:
            self._commit(transactions)
            transactions.clear()

    def _commit(self, transactions: QueueTransactions):
        transactions_data = {}
        for transaction in transactions:
            for key, value in transaction.items():
                transactions_data[key] = value
        for key, value in transactions_data.items():
            if value is None:
                self._data.pop(key, None)
            else:
                self._data[key] = value

    def counts(self, transactions: QueueTransactions, value: str):
        total = 0
        keys: Set[str] = set()
        if transactions:
            total += self._counts(value, transactions[-1], keys)
        total += self._counts(value, self._data, keys)
        return total

    def _counts(self, value: str, storage: Dict[str, Optional[str]], keys: Set[str]):
        count = 0
        for k, v in storage.items():
            if k not in keys:
                keys.add(k)
                if v == value:
                    count += 1
        return count

    @property
    def commands(self):
        return self._commands

    @property
    def data(self):
        return self._data
