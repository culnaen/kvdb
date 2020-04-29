from unittest import TestCase

from kvdb import KVDB


DB = KVDB()


class TestQueries(TestCase):
    def test_commit(self):
        data_for_assert_equal = {"COMMIT_TEST_3": "3"}
        data_for_assert_not_equal = {"COMMIT_TEST_1": "1", "COMMIT_TEST_3": "3", }
        with DB.connect() as conn:
            conn.execute("SET COMMIT_TEST_1 1")
            conn.execute("BEGIN")
            conn.execute("UNSET COMMIT_TEST_1")
            conn.execute("SET COMMIT_TEST_2 2")
            conn.execute("BEGIN")
            conn.execute("UNSET COMMIT_TEST_2")
            conn.execute("SET COMMIT_TEST_3 3")
            conn.execute("COMMIT")
            self.assertEqual(data_for_assert_equal, DB.storage)
            self.assertNotEqual(data_for_assert_not_equal, DB.storage)

    def test_counts_in_transactions(self):
        with DB.connect() as conn:
            conn.execute("SET COUNTS_TEST_1 10")
            conn.execute("BEGIN")
            conn.execute("SET COUNTS_TEST_1 20")
            counts = conn.execute("COUNTS 10")
            self.assertNotEqual(1, counts)
            self.assertEqual(0, counts)
