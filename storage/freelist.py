"""In-memory freelist for tracking available page IDs.

The freelist tracks page IDs that have been freed and can be reused
for new allocations. This is separate from FreelistPage which handles
persistence of the freelist to disk.
"""

from storage.pages import FreelistPage


class Freelist:
    """Manages free page IDs for reuse during page allocation.

    The freelist maintains an in-memory set of page IDs that are available
    for reuse. When pages are freed (e.g., old versions after CoW), they're
    added here. New allocations check the freelist before extending the file.
    """

    def __init__(self, free_page_ids: list[int] | None = None):
        self._free: set[int] = set(free_page_ids or [])

    def allocate(self) -> int | None:
        """Get a free page ID for reuse, or None if freelist is empty."""
        if self._free:
            return self._free.pop()
        return None

    def free(self, page_id: int) -> None:
        """Add a page ID to the freelist for future reuse."""
        self._free.add(page_id)

    def free_many(self, page_ids: list[int]) -> None:
        """Add multiple page IDs to the freelist."""
        self._free.update(page_ids)

    def count(self) -> int:
        """Number of free pages available."""
        return len(self._free)

    def is_empty(self) -> bool:
        """Check if freelist has no pages available."""
        return len(self._free) == 0

    def contains(self, page_id: int) -> bool:
        """Check if a page ID is in the freelist."""
        return page_id in self._free

    def to_list(self) -> list[int]:
        """Get sorted list of free page IDs."""
        return sorted(self._free)

    def to_page(self, page_id: int) -> FreelistPage:
        """Create a FreelistPage for persistence."""
        return FreelistPage(page_id=page_id, free_page_ids=self.to_list())

    @classmethod
    def from_page(cls, page: FreelistPage) -> "Freelist":
        """Load freelist from a persisted FreelistPage."""
        return cls(free_page_ids=page.free_page_ids)

    def clear(self) -> None:
        """Remove all entries from the freelist."""
        self._free.clear()
