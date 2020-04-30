from unittest import TestCase

from kvdb import KVDB


DB = KVDB()


class TestQueries(TestCase):
    def test_commit(self):
        data_for_assert_equal = {"test_commit_3": "3"}
        data_for_assert_not_equal = {"test_commit_1": "1", "test_commit_3": "3", }
        with DB.connect() as conn:
            conn.execute("SET test_commit_1 1")
            conn.execute("BEGIN")
            conn.execute("UNSET test_commit_1")
            conn.execute("SET test_commit_2 2")
            conn.execute("BEGIN")
            conn.execute("UNSET test_commit_2")
            conn.execute("SET test_commit_3 3")
            conn.execute("COMMIT")
            self.assertEqual(data_for_assert_equal, DB.storage)
            self.assertNotEqual(data_for_assert_not_equal, DB.storage)

    def test_key_with_other_value_in_transaction(self):
        with DB.connect() as conn:
            conn.execute("SET test_key_with_other_value_in_transaction_1 4")
            conn.execute("BEGIN")
            conn.execute("SET test_key_with_other_value_in_transaction_1 5")
            counts = conn.execute("COUNTS 10")
            self.assertNotEqual(1, counts)
            self.assertEqual(0, counts)

    def test_duplicate_key_in_transaction_with_same_value(self):
        with DB.connect() as conn:
            conn.execute("BEGIN")
            conn.execute("SET test_duplicate_key_in_transaction_with_same_value_1 6")
            conn.execute("BEGIN")
            conn.execute("SET test_duplicate_key_in_transaction_with_same_value_1 6")
            counts = conn.execute("COUNTS 6")
            self.assertNotEqual(0, counts)
            self.assertEqual(1, counts)

    def test_other_key_in_transaction_with_same_value(self):
        with DB.connect() as conn:
            conn.execute("SET test_other_key_in_transaction_with_same_value_1 7")
            conn.execute("BEGIN")
            conn.execute("SET test_other_key_in_transaction_with_same_value_2 7")
            counts = conn.execute("COUNTS 7")
            self.assertNotEqual(1, counts)
            self.assertEqual(2, counts)
