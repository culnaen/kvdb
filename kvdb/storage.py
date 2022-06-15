from enum import Enum
import threading
from typing import Dict, Optional, Set



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
    def __init__(self, lock: threading.RLock):
        self._lock = lock
        self._data = {}
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
        return "ok"

    def unset(self, transactions: QueueTransactions, key: str):
        if transactions:
            transactions[-1][key] = None
        else:
            self._data.pop(key, None)
        return "ok"

    def begin(self, transactions: QueueTransactions):
        self._lock.acquire()
        transactions.append(dict())
        return "begin"

    def rollback(self, transactions: QueueTransactions):
        transactions.pop()
        self._lock.release()
        return "rollback"

    def commit(self, transactions: QueueTransactions):
        self._commit(transactions)
        transactions.clear()
        self._lock.release()
        return "commit"

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
