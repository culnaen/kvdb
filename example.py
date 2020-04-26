from threading import Thread, Lock

from kvdb import KVDB


def _(db, lock):
    with db.connect() as conn:
        conn.execute("SET 1 1")
        conn.execute("BEGIN")
        conn.execute("SET 2 1")
        count1 = conn.execute("COUNTS 1")
        conn.execute("UNSET 2")
        conn.execute("BEGIN")
        conn.execute("SET 3 1")
        conn.execute("COMMIT")
        count2 = conn.execute("COUNTS 1")
        with lock:
            print(count1, "COUNTS before UNSET")
            print(count2, "COUNTS AFTER COMMIT")


def main():
    lock = Lock()
    db = KVDB()
    threads = [Thread(target=_, args=(db, lock)) for i in range(10)]
    for x in threads:
        x.start()
    for x in threads:
        x.join()


if __name__ == '__main__':
    main()
