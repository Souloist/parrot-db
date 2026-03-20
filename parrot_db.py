"""ParrotDB: LMDB-inspired embedded key-value database.

Provides MVCC transactions via copy-on-write B+ tree with atomic commits
through dual meta page swapping. Single writer, multiple readers.
"""

import shutil
import threading
from pathlib import Path
from typing import Self

from models.storage import PageType
from storage import BTree, Pager
from storage.pages import BranchPage, FreelistPage, LeafPage, MetaPage
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

        # Track active read transactions for deferred page freeing
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
            # Get old meta to find orphaned pages
            old_meta = self._pager.read_active_meta()
            old_root = old_meta.root_page_id
            new_root = txn.root_page_id

            # Find pages that were orphaned by this transaction
            if old_root != new_root and old_root != 0:
                old_pages = self._btree.collect_page_ids(old_root)
                new_pages = self._btree.collect_page_ids(new_root) if new_root != 0 else set()
                orphaned = old_pages - new_pages

                if orphaned:
                    # Mark orphaned pages as pending-free at the old txn_id
                    # They can be freed once no reader can see old_meta.txn_id
                    self._pager.freelist.mark_pending_free(orphaned, old_meta.txn_id)

            # Try to release any pending pages that are no longer needed
            oldest_reader_txn = self._get_oldest_reader_txn_id()
            self._pager.freelist.release_pending(oldest_reader_txn)

            # Persist freelist if it has entries
            freelist_page_id = self._persist_freelist(old_meta.freelist_page_id)

            # Write new root to inactive meta page
            inactive_meta_id = self._pager.get_inactive_meta_id()
            new_meta = MetaPage(
                page_id=inactive_meta_id,
                txn_id=txn.txn_id,
                root_page_id=txn.root_page_id,
                freelist_page_id=freelist_page_id,
            )
            self._pager.write_meta_page(new_meta)

            # Sync to disk - this makes the commit durable
            self._pager.sync()
        finally:
            # Always release write lock, even on failure
            self._active_write_txn = None
            self._write_lock.release()

    def _persist_freelist(self, old_freelist_page_id: int) -> int:
        """Persist the freelist to disk, returning the freelist page ID.

        We reuse the existing freelist page if available, or allocate a new
        page by extending the file (not from the freelist). This avoids the
        circular problem of consuming free pages to store the freelist.

        When the freelist is empty but an old freelist page exists, we write
        an empty freelist to keep the page referenced (prevents leaking one
        page per empty-freelist cycle). Only entries up to max_entries are
        persisted; overflow stays in memory and is recoverable via compaction.
        """
        if self._pager.freelist.count() == 0 and old_freelist_page_id == 0:
            return 0

        if old_freelist_page_id != 0:
            freelist_page_id = old_freelist_page_id
        else:
            freelist_page_id = self._pager.allocate_page_extend()

        max_entries = FreelistPage(page_id=0).max_entries(self._pager.page_size)
        freelist_page = self._pager.freelist.to_page(freelist_page_id, max_entries=max_entries)
        self._pager.write_freelist_page(freelist_page)
        return freelist_page_id

    def _get_oldest_reader_txn_id(self) -> int | None:
        """Get the txn_id of the oldest active reader, or None if no readers."""
        with self._read_txns_lock:
            if not self._active_read_txns:
                return None
            return min(txn.txn_id for txn in self._active_read_txns)

    def _rollback_write_txn(self, txn: WriteTransaction) -> None:
        """Rollback a write transaction, discarding all changes."""
        # Pages allocated during this transaction are orphaned.
        # They'll be reclaimed during compaction since they're not
        # reachable from any committed root.
        self._active_write_txn = None
        self._write_lock.release()

    def _release_read_txn(self, txn: ReadTransaction) -> None:
        """Release a read transaction and try to free pending pages."""
        with self._read_txns_lock:
            self._active_read_txns.discard(txn)
            oldest_reader_txn = (
                min((t.txn_id for t in self._active_read_txns), default=None)
                if self._active_read_txns
                else None
            )

        if self._pager.freelist.pending_count() > 0:
            self._pager.freelist.release_pending(oldest_reader_txn)

    def close(self) -> None:
        """Close the database, persisting any in-memory freelist changes."""
        if self._active_write_txn is not None:
            raise RuntimeError("Cannot close with active write transaction")
        if self._active_read_txns:
            raise RuntimeError("Cannot close with active read transactions")
        self._persist_freelist_on_close()
        self._pager.close()

    def _persist_freelist_on_close(self) -> None:
        """Flush in-memory freelist state to disk before closing.

        Pages released from pending-free (when readers close) only exist in
        memory. Without this, a shutdown before the next write commit would
        lose those freed pages.
        """
        old_meta = self._pager.read_active_meta()
        freelist_page_id = self._persist_freelist(old_meta.freelist_page_id)

        if freelist_page_id != old_meta.freelist_page_id:
            inactive_meta_id = self._pager.get_inactive_meta_id()
            new_meta = MetaPage(
                page_id=inactive_meta_id,
                txn_id=old_meta.txn_id + 1,
                root_page_id=old_meta.root_page_id,
                freelist_page_id=freelist_page_id,
            )
            self._pager.write_meta_page(new_meta)
            self._pager.sync()

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

    def delete(self, key: bytes) -> bool:
        """Delete a key (auto write transaction). Returns True if key existed."""
        with self.begin(write=True) as txn:
            return txn.delete(key)

    # Database copy/compaction

    def copy(self, dest_path: str | Path, compact: bool = False) -> None:
        """Copy the database to a new file.

        Args:
            dest_path: Destination file path
            compact: If True, rewrite the tree contiguously (removes holes).
                    If False, straight file copy preserving layout.

        The copy operates on a read snapshot for consistency.
        """
        dest_path = Path(dest_path)

        if compact:
            self._copy_compact(dest_path)
        else:
            self._copy_raw(dest_path)

    def _copy_raw(self, dest_path: Path) -> None:
        """Copy the database file directly (preserves layout)."""
        with self.begin():
            self._pager.sync()
            shutil.copy2(self._path, dest_path)

    def _copy_compact(self, dest_path: Path) -> None:
        """Copy the database with compaction (contiguous pages, no holes)."""
        with self.begin() as txn:
            root_page_id = txn.root_page_id

            # Create new database file
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with Pager(dest_path, page_size=self._pager.page_size, create=True) as dest_pager:
                if root_page_id == 0:
                    # Empty tree - nothing to copy
                    return

                # Copy tree pages in depth-first order, remapping page IDs
                page_map: dict[int, int] = {}  # old_page_id -> new_page_id
                new_root = self._copy_tree_recursive(root_page_id, dest_pager, page_map)

                # Write meta page with new root
                new_meta = MetaPage(
                    page_id=dest_pager.get_inactive_meta_id(),
                    txn_id=txn.txn_id,
                    root_page_id=new_root,
                    freelist_page_id=0,  # Compacted file has no free pages
                )
                dest_pager.write_meta_page(new_meta)
                dest_pager.sync()

    def _copy_tree_recursive(
        self, page_id: int, dest_pager: Pager, page_map: dict[int, int]
    ) -> int:
        """Recursively copy a tree page and its children, remapping page IDs."""
        if page_id in page_map:
            return page_map[page_id]

        page_data = self._pager.read_page_raw(page_id)
        page_type = page_data[0]

        if page_type == PageType.LEAF:
            leaf = LeafPage.from_bytes(page_data)
            new_page_id = dest_pager.allocate_page()
            # Don't copy right_sibling - it may point to old page IDs
            new_leaf = LeafPage(page_id=new_page_id, cells=leaf.cells, right_sibling=0)
            dest_pager.write_leaf_page(new_leaf)
            page_map[page_id] = new_page_id
            return new_page_id

        elif page_type == PageType.BRANCH:
            branch = BranchPage.from_bytes(page_data)

            # First, recursively copy all children
            new_children = []
            for child_id in branch.children:
                new_child_id = self._copy_tree_recursive(child_id, dest_pager, page_map)
                new_children.append(new_child_id)

            # Then create new branch with remapped children
            new_page_id = dest_pager.allocate_page()
            new_branch = BranchPage(
                page_id=new_page_id,
                keys=list(branch.keys),
                children=new_children,
            )
            dest_pager.write_branch_page(new_branch)
            page_map[page_id] = new_page_id
            return new_page_id

        else:
            raise ValueError(f"Unexpected page type: {page_type}")

    @property
    def path(self) -> Path:
        """Path to the database file."""
        return self._path

    @property
    def freelist_count(self) -> int:
        """Number of free pages available for reuse."""
        return self._pager.freelist.count()

    @property
    def pending_free_count(self) -> int:
        """Number of pages pending free (waiting for readers)."""
        return self._pager.freelist.pending_count()
