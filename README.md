# parrot-db
Toy project of implementing a KV Database which supports transactions (OCC via snapshot isolation) and recovery
via on disk logs

## How to run

```sh
python client.py
```


## Features to improve on

* Support nested transactions (done)
* Support snapshot isolation with data versions instead of copying state 
* Support concurrency with locking on write
* Support persistence to disk with write ahead log (WAL)
* Add separate process to compact WAL into a snapshot