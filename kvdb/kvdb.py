from .storage import Storage
from .connection import Connection


class KVDB:
    def __init__(self):
        self.storage = Storage()

    def connect(self):
        return Connection(self.storage)

    @property
    def db(self):
        return self.storage.db
