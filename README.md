# kvdb

## use
```
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

```


## commands

- SET key value  
  Set key to hold the string value. If key already holds a value, it is overwritten.
  
- GET key  
  Get the value of key. If the key does not exist the special value None is returned.
  
- UNSET key  
  Deletes the specified key.
  
- COUNTS value  
  Returns the number of values.
- BEGIN  
  Start of transaction.

- ROLLBACK  
  Transaction rollback.
  
- COMMIT  
  Merges data from a transaction with the main storage.



