"""Page-based I/O for the storage engine.

File layout:
    Page 0: Header page (magic, version, page_size)
    Page 1: Meta page 0
    Page 2: Meta page 1
    Page 3+: Data pages (freelist, branch, leaf)

The Pager handles all file I/O operations, ensuring pages are read and
written at correct offsets with proper checksums.
"""

import os
from pathlib import Path
from typing import Self

from storage.freelist import Freelist
from storage.pages import (
    DEFAULT_PAGE_SIZE,
    BranchPage,
    FreelistPage,
    HeaderPage,
    LeafPage,
    MetaPage,
)

# Reserved page IDs
HEADER_PAGE_ID = 0
META_PAGE_0_ID = 1
META_PAGE_1_ID = 2
FIRST_DATA_PAGE_ID = 3


class Pager:
    """Manages page-based I/O for the database file.

    Provides methods to read/write pages by page_id, allocate new pages,
    and manage the file layout with header and dual meta pages.
    """

    def __init__(
        self,
        path: Path | str,
        page_size: int = DEFAULT_PAGE_SIZE,
        create: bool = True,
    ):
        self.path = Path(path)
        self.page_size = page_size
        self._file: open | None = None
        self._freelist = Freelist()
        self._next_page_id = FIRST_DATA_PAGE_ID

        if self.path.exists():
            self._open_existing()
        elif create:
            self._create_new()
        else:
            raise FileNotFoundError(f"Database file not found: {self.path}")

    def _create_new(self) -> None:
        """Create a new database file with header and meta pages."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "w+b")

        # Write header page
        header = HeaderPage(page_size=self.page_size)
        self._write_page_raw(HEADER_PAGE_ID, header.to_bytes(self.page_size))

        # Write initial meta pages (both empty)
        meta0 = MetaPage(page_id=META_PAGE_0_ID, txn_id=0)
        meta1 = MetaPage(page_id=META_PAGE_1_ID, txn_id=0)
        self._write_page_raw(META_PAGE_0_ID, meta0.to_bytes(self.page_size))
        self._write_page_raw(META_PAGE_1_ID, meta1.to_bytes(self.page_size))

        self._file.flush()
        self._next_page_id = FIRST_DATA_PAGE_ID

    def _open_existing(self) -> None:
        """Open an existing database file and validate header."""
        self._file = open(self.path, "r+b")

        # Read and validate header
        header_data = self._read_page_raw(HEADER_PAGE_ID)
        header = HeaderPage.from_bytes(header_data)
        self.page_size = header.page_size

        # Determine next page ID from file size
        file_size = self.path.stat().st_size
        self._next_page_id = file_size // self.page_size

        # Load freelist if present
        meta = self.read_active_meta()
        if meta.freelist_page_id != 0:
            freelist_page = self.read_freelist_page(meta.freelist_page_id)
            self._freelist = Freelist.from_page(freelist_page)

    def _page_offset(self, page_id: int) -> int:
        """Calculate file offset for a page ID."""
        return page_id * self.page_size

    def _read_page_raw(self, page_id: int) -> bytes:
        """Read raw bytes for a page."""
        if self._file is None:
            raise RuntimeError("Pager is closed")

        offset = self._page_offset(page_id)
        self._file.seek(offset)
        data = self._file.read(self.page_size)

        if len(data) < self.page_size:
            raise ValueError(f"Incomplete page read at page_id {page_id}")

        return data

    def _write_page_raw(self, page_id: int, data: bytes) -> None:
        """Write raw bytes to a page."""
        if self._file is None:
            raise RuntimeError("Pager is closed")

        if len(data) != self.page_size:
            raise ValueError(f"Page data must be exactly {self.page_size} bytes, got {len(data)}")

        offset = self._page_offset(page_id)
        self._file.seek(offset)
        self._file.write(data)

    def read_header(self) -> HeaderPage:
        """Read the header page."""
        data = self._read_page_raw(HEADER_PAGE_ID)
        return HeaderPage.from_bytes(data)

    def read_meta_page(self, page_id: int, verify_checksum: bool = True) -> MetaPage:
        """Read a meta page by ID (1 or 2)."""
        if page_id not in (META_PAGE_0_ID, META_PAGE_1_ID):
            raise ValueError(f"Invalid meta page ID: {page_id}")

        data = self._read_page_raw(page_id)
        return MetaPage.from_bytes(data, verify_checksum=verify_checksum)

    def read_active_meta(self) -> MetaPage:
        """Read the active meta page (highest valid txn_id)."""
        try:
            meta0 = self.read_meta_page(META_PAGE_0_ID, verify_checksum=True)
        except ValueError:
            meta0 = None

        try:
            meta1 = self.read_meta_page(META_PAGE_1_ID, verify_checksum=True)
        except ValueError:
            meta1 = None

        if meta0 is None and meta1 is None:
            raise ValueError("Both meta pages are invalid")

        if meta0 is None:
            return meta1
        if meta1 is None:
            return meta0

        return meta0 if meta0.txn_id >= meta1.txn_id else meta1

    def write_meta_page(self, meta: MetaPage) -> None:
        """Write a meta page."""
        if meta.page_id not in (META_PAGE_0_ID, META_PAGE_1_ID):
            raise ValueError(f"Invalid meta page ID: {meta.page_id}")

        data = meta.to_bytes(self.page_size)
        self._write_page_raw(meta.page_id, data)

    def get_inactive_meta_id(self) -> int:
        """Get the page ID of the inactive meta page."""
        active = self.read_active_meta()
        return META_PAGE_1_ID if active.page_id == META_PAGE_0_ID else META_PAGE_0_ID

    def read_freelist_page(self, page_id: int, verify_checksum: bool = True) -> FreelistPage:
        """Read a freelist page."""
        data = self._read_page_raw(page_id)
        return FreelistPage.from_bytes(data, verify_checksum=verify_checksum)

    def write_freelist_page(self, page: FreelistPage) -> None:
        """Write a freelist page."""
        data = page.to_bytes(self.page_size)
        self._write_page_raw(page.page_id, data)

    def read_leaf_page(self, page_id: int, verify_checksum: bool = True) -> LeafPage:
        """Read a leaf page."""
        data = self._read_page_raw(page_id)
        return LeafPage.from_bytes(data, verify_checksum=verify_checksum)

    def write_leaf_page(self, page: LeafPage) -> None:
        """Write a leaf page."""
        data = page.to_bytes(self.page_size)
        self._write_page_raw(page.page_id, data)

    def read_branch_page(self, page_id: int, verify_checksum: bool = True) -> BranchPage:
        """Read a branch page."""
        data = self._read_page_raw(page_id)
        return BranchPage.from_bytes(data, verify_checksum=verify_checksum)

    def write_branch_page(self, page: BranchPage) -> None:
        """Write a branch page."""
        data = page.to_bytes(self.page_size)
        self._write_page_raw(page.page_id, data)

    def allocate_page(self) -> int:
        """Allocate a new page ID, reusing from freelist if available."""
        # Try to reuse a free page
        free_id = self._freelist.allocate()
        if free_id is not None:
            return free_id

        # Otherwise extend the file
        page_id = self._next_page_id
        self._next_page_id += 1
        return page_id

    def free_page(self, page_id: int) -> None:
        """Add a page ID to the freelist for future reuse."""
        if page_id < FIRST_DATA_PAGE_ID:
            raise ValueError(f"Cannot free reserved page: {page_id}")
        self._freelist.free(page_id)

    @property
    def freelist(self) -> Freelist:
        """Access the in-memory freelist."""
        return self._freelist

    @property
    def page_count(self) -> int:
        """Total number of pages in the file."""
        return self._next_page_id

    def sync(self) -> None:
        """Flush all writes to disk."""
        if self._file is not None:
            self._file.flush()
            os.fsync(self._file.fileno())

    def close(self) -> None:
        """Close the database file."""
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
