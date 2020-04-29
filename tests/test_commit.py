from unittest import TestCase

from kvdb import KVDB


DB = KVDB()

DATA = {"C": "3"}
DATA1 = {"X": "1", "C": "3", }


class TestCommit(TestCase):
    def test_commit(self):
        with DB.connect() as conn:
            conn.execute("SET X 1")
            conn.execute("BEGIN")
            conn.execute("UNSET X")
            conn.execute("SET Z 2")
            conn.execute("BEGIN")
            conn.execute("UNSET Z")
            conn.execute("SET C 3")
            conn.execute("COMMIT")
            self.assertEqual(DATA, DB.storage)
            self.assertNotEqual(DATA1, DB.storage)
