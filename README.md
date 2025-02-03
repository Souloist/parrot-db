# parrot-db 🦜
Toy project of implementing a KV Database which supports transactions (OCC via snapshot isolation) and recovery
via on disk logs

## How to run

Running `python client.py` will display the following:

```
Welcome to Parrot database!
Commands:
    set <key> <value>   - Sets the value for the given key
    get <key>           - Returns the value for the given key
    count <value>       - Returns number of keys with given value
    delete <key>        - Deletes key
    exit                - Exits the program

    begin               - Begins a transaction. Supported nested transactions
    commit              - Commits current transaction
    rollback            - Rollback current transaction
>
```

## Features to improve on

* Support nested transactions (done)
* Support snapshot isolation with data versions instead of copying state 
* Support concurrency with locking on write
* Support persistence to disk with write ahead log (WAL)
* Add separate process to compact WAL into a snapshot
