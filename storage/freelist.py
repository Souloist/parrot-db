"""Freelist for tracking available and pending-free page IDs.

The freelist manages page reuse with support for MVCC:
- Free pages: immediately available for allocation
- Pending-free pages: pages that can be freed once all readers
  that might reference them have completed (deferred freeing)

This enables snapshot isolation - readers hold a root_page_id and can
safely traverse old tree versions while writers create new pages.
"""

from storage.pages import FreelistPage


class Freelist:
    """Manages free page IDs for reuse during page allocation.

    Supports deferred freeing for MVCC: pages are marked pending-free
    with a txn_id, and only moved to the free set when the oldest
    active reader has advanced past that txn_id.
    """

    def __init__(self, free_page_ids: list[int] | None = None):
        self._free: set[int] = set(free_page_ids or [])
        # pending_free: txn_id -> set of page_ids that can be freed
        # once all readers at or before txn_id have completed
        self._pending_free: dict[int, set[int]] = {}

    def allocate(self) -> int | None:
        """Get a free page ID for reuse, or None if freelist is empty."""
        if self._free:
            return self._free.pop()
        return None

    def free(self, page_id: int) -> None:
        """Add a page ID to the freelist for immediate reuse."""
        self._free.add(page_id)

    def free_many(self, page_ids: list[int]) -> None:
        """Add multiple page IDs to the freelist for immediate reuse."""
        self._free.update(page_ids)

    def mark_pending_free(self, page_ids: set[int], txn_id: int) -> None:
        """Mark pages as pending-free, to be released after txn_id.

        These pages were part of the tree at txn_id but are now replaced.
        They can only be freed once no reader can see txn_id anymore.
        """
        if not page_ids:
            return
        if txn_id not in self._pending_free:
            self._pending_free[txn_id] = set()
        self._pending_free[txn_id].update(page_ids)

    def release_pending(self, oldest_active_txn_id: int | None) -> int:
        """Release pending-free pages that are no longer reachable.

        Pages pending-free at txn_id < oldest_active_txn_id are safe to
        free because no reader can possibly reference them anymore.

        Args:
            oldest_active_txn_id: The lowest txn_id among active readers,
                or None if no readers are active (all pending can be freed)

        Returns:
            Number of pages released to the free set
        """
        released = 0
        txn_ids_to_remove = []

        for txn_id, page_ids in self._pending_free.items():
            # If no active readers, or this txn is older than all readers
            if oldest_active_txn_id is None or txn_id < oldest_active_txn_id:
                self._free.update(page_ids)
                released += len(page_ids)
                txn_ids_to_remove.append(txn_id)

        for txn_id in txn_ids_to_remove:
            del self._pending_free[txn_id]

        return released

    def count(self) -> int:
        """Number of immediately free pages available."""
        return len(self._free)

    def pending_count(self) -> int:
        """Number of pages pending free (waiting for readers)."""
        return sum(len(pages) for pages in self._pending_free.values())

    def is_empty(self) -> bool:
        """Check if freelist has no immediately free pages."""
        return len(self._free) == 0

    def contains(self, page_id: int) -> bool:
        """Check if a page ID is in the free set."""
        return page_id in self._free

    def is_pending(self, page_id: int) -> bool:
        """Check if a page ID is pending free."""
        return any(page_id in pages for pages in self._pending_free.values())

    def to_list(self) -> list[int]:
        """Get sorted list of immediately free page IDs."""
        return sorted(self._free)

    def pending_to_dict(self) -> dict[int, list[int]]:
        """Get pending-free pages as dict of txn_id -> sorted page list."""
        return {txn_id: sorted(pages) for txn_id, pages in self._pending_free.items()}

    def to_page(self, page_id: int, max_entries: int | None = None) -> FreelistPage:
        """Create a FreelistPage for persistence (free pages only).

        Note: pending-free pages are not persisted to the freelist page.
        On recovery, orphaned pages from uncommitted transactions are
        handled by tree traversal during compaction.
        """
        entries = self.to_list()
        if max_entries is not None:
            entries = entries[:max_entries]
        return FreelistPage(page_id=page_id, free_page_ids=entries)

    @classmethod
    def from_page(cls, page: FreelistPage) -> "Freelist":
        """Load freelist from a persisted FreelistPage."""
        return cls(free_page_ids=page.free_page_ids)

    def clear(self) -> None:
        """Remove all entries from the freelist."""
        self._free.clear()
        self._pending_free.clear()
