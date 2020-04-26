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
            for key, value in transactions[-1].items():
                if value is None:
                    del self._data[value]
                else:
                    self._data[key] = value
                transactions.pop()

    def counts(self, transactions, value):
        total = 0
        if transactions:
            for k, v in transactions[-1].items():
                if v == value and k not in self._data:
                    total += 1
        for k, v in self._data.items():
            if v == value:
                total += 1
        return total

    @property
    def commands(self):
        return self._commands

    @property
    def db(self):
        return self._data
