"""ParrotDB: LMDB-inspired embedded key-value database.

Provides MVCC transactions via copy-on-write B+ tree with atomic commits
through dual meta page swapping. Single writer, multiple readers.
"""

import threading
from pathlib import Path
from typing import Self

from storage import BTree, Pager
from storage.pages import MetaPage
from txn import ReadTransaction, WriteTransaction


class ParrotDB:
    """Embedded key-value database with MVCC transactions.

    Usage:
        db = ParrotDB(path="./data.db")

        # Read transaction
        with db.begin() as txn:
            value = txn.get(b"key")

        # Write transaction
        with db.begin(write=True) as txn:
            txn.put(b"key", b"value")
            txn.commit()

        db.close()
    """

    def __init__(self, path: str | Path, create: bool = True):
        """Open or create a database.

        Args:
            path: Path to database file
            create: If True, create database if it doesn't exist
        """
        self._path = Path(path)
        self._pager = Pager(self._path, create=create)
        self._btree = BTree(self._pager)

        # Single writer lock
        self._write_lock = threading.Lock()
        self._active_write_txn: WriteTransaction | None = None

        # Track active read transactions for future freelist management
        self._read_txns_lock = threading.Lock()
        self._active_read_txns: set[ReadTransaction] = set()

    def begin(self, write: bool = False) -> ReadTransaction | WriteTransaction:
        """Begin a new transaction.

        Args:
            write: If True, start a write transaction (exclusive)
                   If False, start a read transaction (shared)

        Returns:
            ReadTransaction or WriteTransaction with context manager support

        Raises:
            RuntimeError: If write=True and another write transaction is active
        """
        meta = self._pager.read_active_meta()
        root_page_id = meta.root_page_id
        txn_id = meta.txn_id

        if write:
            if not self._write_lock.acquire(blocking=False):
                raise RuntimeError("Another write transaction is active")
            self._active_write_txn = WriteTransaction(self, root_page_id, txn_id + 1)
            return self._active_write_txn
        else:
            txn = ReadTransaction(self, root_page_id, txn_id)
            with self._read_txns_lock:
                self._active_read_txns.add(txn)
            return txn

    def _commit_write_txn(self, txn: WriteTransaction) -> None:
        """Commit a write transaction atomically."""
        try:
            # Write new root to inactive meta page
            inactive_meta_id = self._pager.get_inactive_meta_id()
            new_meta = MetaPage(
                page_id=inactive_meta_id,
                txn_id=txn.txn_id,
                root_page_id=txn.root_page_id,
                freelist_page_id=0,  # Freelist persistence is Stage 5
            )
            self._pager.write_meta_page(new_meta)

            # Sync to disk - this makes the commit durable
            self._pager.sync()
        finally:
            # Always release write lock, even on failure
            self._active_write_txn = None
            self._write_lock.release()

    def _rollback_write_txn(self, txn: WriteTransaction) -> None:
        """Rollback a write transaction, discarding all changes."""
        # Simply release the lock - pages written but not committed
        # will be orphaned and eventually reclaimed (Stage 5)
        self._active_write_txn = None
        self._write_lock.release()

    def _release_read_txn(self, txn: ReadTransaction) -> None:
        """Release a read transaction."""
        with self._read_txns_lock:
            self._active_read_txns.discard(txn)

    def close(self) -> None:
        """Close the database."""
        if self._active_write_txn is not None:
            raise RuntimeError("Cannot close with active write transaction")
        if self._active_read_txns:
            raise RuntimeError("Cannot close with active read transactions")
        self._pager.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # Convenience methods for simple operations without explicit transactions

    def get(self, key: bytes) -> bytes | None:
        """Get a value by key (auto read transaction)."""
        with self.begin() as txn:
            return txn.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        """Put a key-value pair (auto write transaction)."""
        with self.begin(write=True) as txn:
            txn.put(key, value)
            txn.commit()

    def delete(self, key: bytes) -> bool:
        """Delete a key (auto write transaction). Returns True if key existed."""
        with self.begin(write=True) as txn:
            result = txn.delete(key)
            txn.commit()
            return result
