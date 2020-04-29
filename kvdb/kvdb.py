from .storage import Storage
from .connection import Connection


class KVDB:
    def __init__(self):
        self._storage = Storage()

    def connect(self):
        return Connection(self._storage)

    @property
    def storage(self):
        return self._storage.data
