"""Transaction classes for snapshot isolation.

Provides ReadTransaction and WriteTransaction with context manager support.
Readers see a consistent snapshot at their start time. Writers accumulate
changes and commit atomically via meta page swap.
"""

from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parrot_db import ParrotDB


class ReadTransaction:
    """Read-only transaction providing snapshot isolation.

    Holds a reference to the root_page_id at transaction start, ensuring
    consistent reads even while other transactions modify the tree.
    """

    def __init__(self, db: "ParrotDB", root_page_id: int, txn_id: int):
        self._db = db
        self._root_page_id = root_page_id
        self._txn_id = txn_id
        self._active = True

    @property
    def root_page_id(self) -> int:
        return self._root_page_id

    @property
    def txn_id(self) -> int:
        return self._txn_id

    def get(self, key: bytes) -> bytes | None:
        """Look up a key in this transaction's snapshot."""
        if not self._active:
            raise RuntimeError("Transaction is no longer active")
        return self._db._btree.get(self._root_page_id, key)

    def range_scan(self, start: bytes | None = None, end: bytes | None = None) -> Iterator[tuple[bytes, bytes]]:
        """Iterate over key-value pairs in sorted order."""
        if not self._active:
            raise RuntimeError("Transaction is no longer active")
        yield from self._db._btree.range_scan(self._root_page_id, start, end)

    def __enter__(self) -> "ReadTransaction":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._close()

    def close(self) -> None:
        """Public close to release transaction resources."""
        self._close()

    def _close(self) -> None:
        """Mark transaction as inactive and notify database."""
        if self._active:
            self._active = False
            self._db._release_read_txn(self)


class WriteTransaction:
    """Read-write transaction with atomic commit.

    Accumulates changes via copy-on-write B+ tree operations. On commit,
    writes the new root to the inactive meta page, fsyncs, and swaps
    the active meta pointer. Rollback discards all changes.
    """

    def __init__(self, db: "ParrotDB", root_page_id: int, txn_id: int):
        self._db = db
        self._root_page_id = root_page_id
        self._txn_id = txn_id
        self._active = True
        self._committed = False

    @property
    def root_page_id(self) -> int:
        return self._root_page_id

    @property
    def txn_id(self) -> int:
        return self._txn_id

    def get(self, key: bytes) -> bytes | None:
        """Look up a key in this transaction's current state."""
        if not self._active:
            raise RuntimeError("Transaction is no longer active")
        return self._db._btree.get(self._root_page_id, key)

    def put(self, key: bytes, value: bytes) -> None:
        """Insert or update a key-value pair."""
        if not self._active:
            raise RuntimeError("Transaction is no longer active")
        self._root_page_id = self._db._btree.insert(self._root_page_id, key, value)

    def delete(self, key: bytes) -> bool:
        """Delete a key. Returns True if key existed, False otherwise."""
        if not self._active:
            raise RuntimeError("Transaction is no longer active")
        old_root = self._root_page_id
        self._root_page_id = self._db._btree.delete(self._root_page_id, key)
        return self._root_page_id != old_root

    def range_scan(self, start: bytes | None = None, end: bytes | None = None) -> Iterator[tuple[bytes, bytes]]:
        """Iterate over key-value pairs in sorted order."""
        if not self._active:
            raise RuntimeError("Transaction is no longer active")
        yield from self._db._btree.range_scan(self._root_page_id, start, end)

    def commit(self) -> None:
        """Commit transaction atomically via meta page swap."""
        if not self._active:
            raise RuntimeError("Transaction is no longer active")
        if self._committed:
            raise RuntimeError("Transaction already committed")

        try:
            self._db._commit_write_txn(self)
        except Exception:
            self._active = False
            raise
        else:
            self._committed = True
            self._active = False

    def rollback(self) -> None:
        """Discard all changes made in this transaction."""
        if not self._active:
            raise RuntimeError("Transaction is no longer active")
        self._db._rollback_write_txn(self)
        self._active = False

    def close(self) -> None:
        """Close is not supported for write transactions.

        Write transactions must be explicitly committed or rolled back.
        Use commit() to persist changes or rollback() to discard them.
        """
        if self._active:
            raise RuntimeError("WriteTransaction must be committed or rolled back, not closed")

    def __enter__(self) -> "WriteTransaction":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._active:
            if exc_type is None and not self._committed:
                # No exception and not committed - auto-commit
                self.commit()
            else:
                # Exception occurred - rollback
                self.rollback()
