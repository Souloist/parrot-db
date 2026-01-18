# parrot-db Progress

## Stage Checklist

| Stage | Description | Status |
|-------|-------------|--------|
| 1 | Serialization layer with Pydantic | Complete |
| 2 | Page-based storage and file layout | Complete |
| 3 | Copy-on-write B+ tree | Complete |
| 4 | Transactions with atomic commits | Complete |
| 5 | Freelist, page reclamation, and compaction | Not started |
| 6 | Crash recovery and durability | Not started |
| 7 | Write-ahead log (optional) | Not started |
| 8 | Memory-mapped I/O (optional) | Not started |

## Current Stage

**Stage:** 4 complete, ready for Stage 5

## Notes

### Stage 1 Notes
- Used `struct` module for binary serialization (user's choice over msgpack)
- Pydantic v2 with `ClassVar` for class constants
- Each model file is self-contained with format constants and struct reference in docstring
- All models support `to_bytes()` / `from_bytes()` round-trip
- 35 tests covering: round-trip, edge cases (empty, large, binary data), validation errors

**Key decisions:**
- Models placed in root `models/` directory (not inside `parrot_db/`)
- Constants co-located in each model file for better context (not centralized)
- Little-endian byte order for all formats

### Stage 2 Notes
- Implemented page-based storage with fixed 4KB pages (configurable)
- File layout: Header (page 0) → Meta 0 (page 1) → Meta 1 (page 2) → Data pages
- Dual meta pages enable atomic commits via alternating writes
- All page types use CRC32 checksums computed over entire page content
- Leaf pages use slot-based layout: cells grow backward from end, offsets grow forward
- Branch pages interleave children and keys for compact representation
- In-memory freelist tracks available pages; persists to FreelistPage
- 44 new tests (79 total) covering round-trip, checksum validation, corruption detection

**Key decisions:**
- Renamed `tools/inspect.py` to `tools/db_inspect.py` to avoid stdlib conflict
- Checksum covers entire page (including padding) for robust corruption detection
- Pager manages file handle lifecycle with context manager support
- Reserved pages (0-2) cannot be freed to protect file structure

---

## Completed Stages

### Stage 1 - Serialization Layer
- **Commit:** _uncommitted - ready for review_
- **Tag:** _pending_
- **Date:** 2026-01-11
- **Files created:**
  - `models/__init__.py` - Package exports
  - `models/storage.py` - KeyValue, PageHeader, PageType
  - `models/wal.py` - WALEntry, WALOperation
  - `models/metadata.py` - DBMetadata, MAGIC, DEFAULT_PAGE_SIZE
  - `tests/test_models.py` - 35 unit tests
- **Tests:** 35 passed

### Stage 2 - Page-based Storage
- **Commit:** _uncommitted - ready for review_
- **Tag:** _pending_
- **Date:** 2026-01-11
- **Files created:**
  - `storage/__init__.py` - Storage layer exports
  - `storage/pager.py` - Pager class for page I/O
  - `storage/pages.py` - HeaderPage, MetaPage, LeafPage, BranchPage, FreelistPage
  - `storage/freelist.py` - In-memory Freelist class
  - `tools/db_inspect.py` - Database inspection CLI
  - `tests/test_pager.py` - 44 unit tests
- **Tests:** 79 passed (35 Stage 1 + 44 Stage 2)

### Stage 3 Notes
- Implemented copy-on-write B+ tree with path copying for MVCC support
- All mutations return new root_page_id; original tree remains valid for readers
- Used in-order tree traversal for range scans instead of sibling pointers (CoW makes maintaining sibling chains complex)
- Leaf/branch splitting detected by calculating exact byte size, not available_space() (which clips to 0)
- 37 new tests (118 total) covering insert, get, delete, range scan, splitting, CoW semantics

**Key decisions:**
- Keys and values are bytes (binary-safe)
- Range scan uses recursive in-order traversal rather than leaf sibling pointers
- No leaf merging on delete (leaves can become sparse; compaction handles this)
- BTree class is stateless - caller manages root_page_id

### Stage 3
- **Commit:** _uncommitted - ready for review_
- **Tag:** _pending_
- **Date:** 2026-01-11
- **Files created:**
  - `storage/btree.py` - Copy-on-write B+ tree implementation
  - `tests/test_btree.py` - 37 unit tests
- **Files modified:**
  - `storage/__init__.py` - Export BTree class
  - `tools/db_inspect.py` - Add tree inspection with --tree flag
- **Tests:** 118 passed (79 Stage 2 + 37 Stage 3 + 2 existing)

### Stage 4 Notes
- Implemented transaction layer with snapshot isolation via CoW B+ tree
- ReadTransaction holds root_page_id at start time for consistent snapshot reads
- WriteTransaction accumulates changes via path copying, commits atomically via meta page swap
- Single writer enforced via threading.Lock; multiple readers allowed concurrently
- Auto-commit on context manager exit if no explicit commit/rollback; auto-rollback on exception
- 27 new tests (151 total) covering commit, rollback, snapshot isolation, persistence

**Key decisions:**
- Transactions are context managers with auto-commit behavior (matches SQLite pattern)
- ParrotDB convenience methods (get/put/delete) auto-create single-operation transactions
- Freelist persistence deferred to Stage 5 (freelist_page_id=0 in meta for now)
- REPL client updated to use persistent storage backend

### Stage 4
- **Commit:** _uncommitted - ready for review_
- **Tag:** _pending_
- **Date:** 2026-01-14
- **Files created:**
  - `txn/__init__.py` - Transaction layer exports
  - `txn/transaction.py` - ReadTransaction, WriteTransaction classes
  - `parrot_db.py` - ParrotDB database class with transaction API
  - `tests/test_transactions.py` - 27 integration tests
- **Files modified:**
  - `client.py` - Updated REPL to use persistent ParrotDB backend
  - `README.md` - Added mermaid architecture diagrams and usage examples
- **Tests:** 151 passed (118 Stage 3 + 27 Stage 4 + 6 existing)

### Stage 5
- **Commit:** _pending_
- **Tag:** _pending_
- **Date:** _pending_

### Stage 6
- **Commit:** _pending_
- **Tag:** _pending_
- **Date:** _pending_

### Stage 7
- **Commit:** _pending_
- **Tag:** _pending_
- **Date:** _pending_

### Stage 8
- **Commit:** _pending_
- **Tag:** _pending_
- **Date:** _pending_