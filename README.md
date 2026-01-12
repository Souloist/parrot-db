# parrot-db 🦜

A toy LMDB-inspired key-value storage engine in Python. [Copy-on-write B+ trees](https://www.bzero.se/ldapd/btree.html) 
are a really elegant data structure which allow us to get database-grade features like MVCC (snapshot isolation) and 
atomic durability without needing a buffer pool or a WAL. A couple downsides however, include limited write throughput
with single writer, poor space efficiency (requiring routine compaction) and bad random I/O performance without an explicit
buffer pool (leverage the OS page cache with mmap)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client API / REPL                        │
│                 (get, put, delete, txn)                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                  Transaction Manager                        │
│           Single writer, multiple readers (MVCC)            │
│        Readers hold root pointer = consistent snapshot      │
└─────────────────────────┬───────────────────────────────────┘
                          │
         ┌────────────────┴────────────────┐
         │                                 │
         ▼                                 ▼
┌──────────────────┐              ┌──────────────────┐
│  CoW B+ Tree     │              │   Dual Meta      │
│                  │              │     Pages        │
│  writes create   │              │                  │
│  new path from   │◄────────────►│  atomic commit   │
│  leaf to root    │              │  via page swap   │
│                  │              │                  │
└────────┬─────────┘              └────────┬─────────┘
         │                                 │
         └────────────────┬────────────────┘
                          │
         ┌────────────────▼────────────────┐
         │           Pager                 │
         │   fixed-size pages (4KB)        │
         │   checksum validation           │
         │   freelist management           │
         └────────────────┬────────────────┘
                          │
                          ▼
                   [ Data File ]
```

**Key Components:**

| Component | Purpose |
|-----------|---------|
| **Transaction Manager** | Single writer with concurrent readers; readers get root pointer at txn start for snapshot isolation |
| **CoW B+ Tree** | Copy-on-write B+ tree for O(log n) key lookup and range scans; mutations copy path from leaf to new root, old tree remains valid for existing readers |
| **Dual Meta Pages** | Two alternating meta pages store root pointer + txn_id; atomic commit = write new root to inactive page, fsync, flip active; crash recovery picks highest valid txn_id |
| **Pager** | Fixed-size page I/O with allocation and checksum validation |
| **Freelist** | Tracks pages to reclaim; deferred freeing ensures old pages remain valid until all readers using them complete |

**Data Flow (Write Path):**
1. Write txn begins → acquires writer lock, reads current root
2. Mutations create new pages (CoW) → builds new tree path
3. Commit → write all new pages, fsync
4. Write new root to inactive meta page, fsync
5. Flip active meta pointer → commit visible to new readers
6. Old pages added to pending-free list → reclaimed when no readers reference them

**Why no WAL?**

LMDB-style CoW provides atomicity without WAL:
- Pages are never modified in place—new versions written to new locations
- Meta page swap is atomic (single sector write)
- Crash at any point → recover by reading valid meta page with highest txn_id
- WAL available as optional enhancement for batching multiple commits

**Space reclamation:**
- Freed pages go to freelist for reuse by future writes
- File doesn't shrink automatically (freed pages leave holes)
- Offline compaction (`tools/compact.py`) rewrites database with only live pages
- Future: auto-compact trigger when freelist exceeds configurable threshold

## Setup

```bash
uv venv
uv sync
```

## How to run

```bash
uv run python client.py
```

This will display the following:

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

## Tests

```bash
uv run pytest
```

## How to inspect

```bash
# Show database summary
uv run python tools/db_inspect.py --db ./tmp/dev.db --summary

# Show specific page
uv run python tools/db_inspect.py --db ./tmp/dev.db --page 3

# Show B+ tree structure (Stage 3+)
uv run python tools/db_inspect.py --db ./tmp/dev.db --tree

# Show freelist
uv run python tools/db_inspect.py --db ./tmp/dev.db --freelist
```

## Features to improve on

- Client for naive implementation to support nested transactions (done)
- Serialization Schema (done)
- Page-based storage with dual meta pages (done)
- Copy-on-write B+ tree
- Transactions with atomic commits
- Freelist and offline manual compaction
- Crash recovery
- WAL for batched durability (optional)
- Memory-mapped I/O (optional)

## Learning Notes

### Checkpoint: Stage 2
- **What we added:** Page-based storage with Pager, page types (Header, Meta, Leaf, Branch, Freelist), and db_inspect tool
- **Key design choice:** Checksums cover entire page including padding, ensuring any byte corruption is detected
- **Edge case to remember:** File named `inspect.py` conflicts with Python stdlib; renamed to `db_inspect.py`
- **How to inspect it:** `uv run python tools/db_inspect.py --db ./tmp/dev.db --summary`
- **What I'd improve next:** Add debug mode flag to Pager that logs page allocations and writes
