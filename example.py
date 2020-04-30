from kvdb import KVDB


def main():
    db = KVDB()
    with db.connect() as conn:
        conn.execute("SET mykey myvalue")  # Set mykey
        conn.execute("GET mykey")  # Return "myvalue"
        conn.execute("COUNTS myvalue")  # Return 1
        conn.execute("UNSET mykey")  # Delete mykey
        conn.execute("GET mykey")  # Return None


if __name__ == '__main__':
    main()
