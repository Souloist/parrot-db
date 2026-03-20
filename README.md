# parrot-db

A toy [LMDB](http://www.lmdb.tech/doc/)-inspired key-value storage engine built in Python for learning database internals.

If you've ever wondered how databases provide crash safety, concurrent reads, or atomic commits — without a write-ahead log — this project walks through those mechanics one piece at a time using [copy-on-write B+ trees](https://www.bzero.se/ldapd/btree.html).

## What you'll learn

Building (or reading through) parrot-db covers several foundational database concepts:

- **Page-based storage** — how databases organize data into fixed-size pages on disk, with checksums for integrity
- **B+ tree indexing** — how sorted key-value lookups, inserts, deletes, and range scans work in O(log n)
- **Copy-on-write (CoW)** — how path copying creates new tree versions without modifying existing pages, giving you MVCC "for free"
- **MVCC & snapshot isolation** — how readers see a consistent snapshot while a writer mutates the tree concurrently
- **Atomic commits** — how dual meta page swapping + fsync provides durability without a WAL
- **Page reclamation** — how freed pages are tracked and safely reused without breaking active readers (deferred freeing)
- **Compaction** — how to reclaim disk space by rewriting only the live pages into a new file

## Architecture

```
                     Client API / REPL
                   (get, put, delete, txn)
                            |
                   Transaction Manager
            single writer, multiple readers (MVCC)
          readers hold root pointer = consistent snapshot
                            |
              +-------------+-------------+
              |                           |
        CoW B+ Tree                 Dual Meta Pages

      writes create new             atomic commit
      path from leaf to root        via page swap
              |                           |
              +-------------+-------------+
                            |
                          Pager
                  fixed-size pages (4KB)
                  checksum validation
                  freelist management
                            |
                       [ Data File ]
```

**Write path:**
1. Write txn begins — acquire writer lock, read current root
2. Mutations create new pages via CoW — new path from leaf to root
3. Commit — write all new pages, fsync
4. Write new root to inactive meta page, fsync — commit is now durable
5. Old pages added to pending-free list — reclaimed when no readers reference them

**Why no WAL?** LMDB-style CoW provides atomicity without one. Pages are never modified in place — new versions go to new locations. The meta page swap is a single-sector write. Crash at any point and recovery just reads the valid meta page with the highest txn_id.

## Project structure

```
parrot-db/
├── models/                  # Pydantic models for on-disk structures
│   ├── storage.py          # KeyValue, PageHeader, PageType
│   ├── wal.py              # WALEntry, WALOperation
│   └── metadata.py         # DBMetadata, MAGIC
├── storage/                 # Page-based storage layer
│   ├── pager.py            # Page I/O, allocation, file layout
│   ├── pages.py            # Page types: Header, Meta, Leaf, Branch, Freelist
│   ├── freelist.py         # Free page tracking with MVCC-aware deferred freeing
│   └── btree.py            # Copy-on-write B+ tree
├── txn/                     # Transaction layer
│   └── transaction.py      # ReadTransaction, WriteTransaction
├── tools/
│   ├── db_inspect.py       # Database inspection CLI
│   └── db_copy.py          # Database copy/compaction CLI
├── parrot_db.py            # Main database class
├── learnings.md            # Design decisions and implementation notes
└── storage-engine-spec.md  # Staged development plan
```

## Getting started

```bash
uv venv && uv sync
```

### Interactive REPL

```bash
uv run python client.py
```

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

### Programmatic usage

```python
from parrot_db import ParrotDB

db = ParrotDB("my.db")

# Simple operations
db.put(b"hello", b"world")
print(db.get(b"hello"))  # b"world"

# Explicit transactions with snapshot isolation
with db.begin(write=True) as txn:
    txn.put(b"key1", b"value1")
    txn.put(b"key2", b"value2")
    txn.commit()

# Readers see a consistent snapshot
with db.begin() as txn:
    for key, value in txn.scan():
        print(key, value)

db.close()
```

### Inspecting the database

```bash
uv run python tools/db_inspect.py --db ./tmp/dev.db --summary   # Overview
uv run python tools/db_inspect.py --db ./tmp/dev.db --tree      # B+ tree structure
uv run python tools/db_inspect.py --db ./tmp/dev.db --page 3    # Specific page
uv run python tools/db_inspect.py --db ./tmp/dev.db --freelist  # Free pages
```

### Copy and compaction

```bash
uv run python tools/db_copy.py source.db backup.db       # Raw copy
uv run python tools/db_copy.py source.db compact.db -c   # Compact (removes holes)
```

## Tests

```bash
uv run pytest
```

## Trade-offs of CoW B+ trees

This design gets you MVCC and crash safety with relatively simple code, but it comes with real trade-offs worth understanding:

| Strength | Limitation |
|----------|------------|
| Snapshot isolation for free (readers just hold a root pointer) | Single writer — only one write txn at a time |
| Atomic commits without WAL (meta page swap) | Write amplification — every mutation copies an entire root-to-leaf path |
| Simple crash recovery (pick highest valid meta page) | Space amplification — old pages aren't freed until all readers finish |
| No buffer pool needed (OS page cache via file I/O) | File doesn't shrink — compaction required to reclaim space |

## Further reading

- [LMDB technical docs](http://www.lmdb.tech/doc/) — the direct inspiration for this project
- [Ohad Rodeh — B-trees, Shadowing, and Clones](https://doi.org/10.1145/1326542.1326544) — the paper behind CoW-friendly B-trees in btrfs
- [Howard Chu — The Lightning Memory-Mapped Database (talk)](https://www.youtube.com/watch?v=tEa5sAh-kVk) — LMDB author explaining the design
- [SQLite database file format](https://www.sqlite.org/fileformat2.html) — a different approach to the same problems

## Learning notes

See [learnings.md](learnings.md) for detailed design decisions, including:
- Why CoW trees avoid leaf sibling pointers (and what we tried before cursor stacks)
- How byte-size splitting fixes a subtle B+ tree overflow bug
- How snapshot isolation falls out naturally from CoW
