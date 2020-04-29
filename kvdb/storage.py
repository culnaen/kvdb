from threading import Lock
from enum import Enum


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

    def get(self, transactions, key):
        return self._get(transactions, key)

    def _get(self, transactions, key):
        if transactions:
            result = transactions[-1].get(key)
            if result is not None:
                return result
        return self._data.get(key)

    def set(self, transactions, key, value):
        if transactions:
            transactions[-1][key] = value
        else:
            self._data[key] = value

    def unset(self, transactions, key):
        if transactions:
            transactions[-1][key] = None
        else:
            del self._data[key]

    def begin(self, transactions):
        transactions.append(dict())

    def rollback(self, transactions):
        transactions.pop()

    def commit(self, transactions):
        with self._lock:
            self._commit(transactions)
            transactions.clear()

    def _commit(self, transactions):
        transactions_data = {}
        for transaction in transactions:
            for key, value in transaction.items():
                transactions_data[key] = value
        for key, value in transactions_data.items():
            if value is None:
                self._data.pop(key, None)
            else:
                self._data[key] = value

    def counts(self, transactions, value):
        total = 0
        keys = set()
        if transactions:
            total += self._counts(value, transactions[-1], keys)
        total += self._counts(value, self._data, keys)
        return total

    def _counts(self, value, storage, keys):
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
