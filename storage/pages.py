"""Page encoding and decoding for all page types.

File layout:
    Page 0: Header page (magic, version, page_size)
    Page 1: Meta page 0 (root_page_id, freelist_page_id, txn_id)
    Page 2: Meta page 1 (alternating meta page)
    Page 3+: Data pages (freelist, branch, leaf)

Each page (except header) starts with a 9-byte PageHeader:
    [page_type:1][page_id:4][checksum:4]

Struct format reference:
    <  = little-endian
    B  = unsigned char (1 byte)
    H  = unsigned short (2 bytes)
    I  = unsigned int (4 bytes)
    Q  = unsigned long long (8 bytes)
"""

import struct
import zlib
from typing import ClassVar, Self

from pydantic import BaseModel, ConfigDict, model_validator

from models import PageHeader, PageType
from models.metadata import MAGIC

DEFAULT_PAGE_SIZE = 4096

# Header page format: [magic:4][version:4][page_size:4][checksum:4] = 16 bytes
HEADER_PAGE_FMT = "<4sIII"
HEADER_PAGE_SIZE = 16

# Meta page format (after PageHeader): [txn_id:8][root_page_id:4][freelist_page_id:4] = 16 bytes
META_PAGE_FMT = "<QII"
META_PAGE_DATA_SIZE = 16

# Freelist page format (after PageHeader): [count:4][page_ids:4*count]
FREELIST_COUNT_FMT = "<I"

# Leaf page format (after PageHeader): [cell_count:2][right_sibling:4][cell_offsets...][cells...]
LEAF_HEADER_FMT = "<HI"
LEAF_HEADER_SIZE = 6

# Branch page format (after PageHeader): [key_count:2][children:4*(key_count+1)][keys...]
BRANCH_HEADER_FMT = "<H"
BRANCH_HEADER_SIZE = 2


def compute_checksum(data: bytes) -> int:
    """Compute CRC32 checksum of data."""
    return zlib.crc32(data) & 0xFFFFFFFF


class HeaderPage(BaseModel):
    """Database file header (page 0).

    Contains magic bytes to identify this as a parrot-db file,
    format version for compatibility, page size, and checksum.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    SIZE: ClassVar[int] = HEADER_PAGE_SIZE

    magic: bytes = MAGIC
    version: int = 2
    page_size: int = DEFAULT_PAGE_SIZE

    def to_bytes(self, page_size: int | None = None) -> bytes:
        """Serialize to a full page with checksum."""
        size = page_size or self.page_size

        # Pack with placeholder checksum (0)
        data_without_checksum = struct.pack("<4sII", self.magic, self.version, self.page_size)

        # Compute checksum over header data only (not padding)
        checksum = compute_checksum(data_without_checksum)

        # Pack with actual checksum
        data = struct.pack(HEADER_PAGE_FMT, self.magic, self.version, self.page_size, checksum)
        return data.ljust(size, b"\x00")

    @classmethod
    def from_bytes(cls, data: bytes, verify_checksum: bool = True) -> Self:
        """Deserialize from bytes, optionally verifying checksum."""
        if len(data) < HEADER_PAGE_SIZE:
            raise ValueError(f"Data too short: expected at least {HEADER_PAGE_SIZE} bytes")

        magic, version, page_size, checksum = struct.unpack(HEADER_PAGE_FMT, data[:HEADER_PAGE_SIZE])

        if magic != MAGIC:
            raise ValueError(f"Invalid magic bytes: expected {MAGIC!r}, got {magic!r}")

        if verify_checksum:
            # Checksum covers header data only (magic, version, page_size)
            data_without_checksum = struct.pack("<4sII", magic, version, page_size)
            expected = compute_checksum(data_without_checksum)
            if checksum != expected:
                raise ValueError(f"Checksum mismatch: expected {expected}, got {checksum}")

        return cls(magic=magic, version=version, page_size=page_size)


class MetaPage(BaseModel):
    """Meta page storing root pointers and transaction ID.

    Two meta pages alternate for atomic commits. On commit, the inactive
    meta page is updated with the new root pointer, then becomes active.
    Recovery picks the meta page with the highest valid txn_id.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    page_id: int
    txn_id: int = 0
    root_page_id: int = 0
    freelist_page_id: int = 0

    def to_bytes(self, page_size: int = DEFAULT_PAGE_SIZE) -> bytes:
        """Serialize to a full page with header and checksum."""
        # Pack meta data
        meta_data = struct.pack(META_PAGE_FMT, self.txn_id, self.root_page_id, self.freelist_page_id)

        # Create header with placeholder checksum
        header = PageHeader(page_type=PageType.META, page_id=self.page_id, checksum=0)
        header_bytes = header.to_bytes()

        # Assemble full page first (with padding)
        full_page = (header_bytes + meta_data).ljust(page_size, b"\x00")

        # Compute checksum over entire page
        checksum = compute_checksum(full_page)

        # Repack header with actual checksum
        header = PageHeader(page_type=PageType.META, page_id=self.page_id, checksum=checksum)
        header_bytes = header.to_bytes()

        return header_bytes + full_page[PageHeader.SIZE :]

    @classmethod
    def from_bytes(cls, data: bytes, verify_checksum: bool = True) -> Self:
        """Deserialize from bytes, optionally verifying checksum."""
        header = PageHeader.from_bytes(data)

        if header.page_type != PageType.META:
            raise ValueError(f"Expected META page, got {header.page_type}")

        meta_offset = PageHeader.SIZE
        if len(data) < meta_offset + META_PAGE_DATA_SIZE:
            raise ValueError("Data too short for meta page")

        txn_id, root_page_id, freelist_page_id = struct.unpack(
            META_PAGE_FMT, data[meta_offset : meta_offset + META_PAGE_DATA_SIZE]
        )

        if verify_checksum:
            # Compute checksum over entire page (with zero checksum in header)
            header_with_zero = PageHeader(page_type=PageType.META, page_id=header.page_id, checksum=0)
            check_data = header_with_zero.to_bytes() + data[PageHeader.SIZE :]
            expected = compute_checksum(check_data)
            if header.checksum != expected:
                raise ValueError(f"Checksum mismatch: expected {expected}, got {header.checksum}")

        return cls(
            page_id=header.page_id,
            txn_id=txn_id,
            root_page_id=root_page_id,
            freelist_page_id=freelist_page_id,
        )


class FreelistPage(BaseModel):
    """Page storing list of free page IDs available for reuse.

    When pages are freed (e.g., after CoW creates new versions),
    their IDs are added here for future allocations.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    page_id: int
    free_page_ids: list[int] = []

    def to_bytes(self, page_size: int = DEFAULT_PAGE_SIZE) -> bytes:
        """Serialize to a full page with header and checksum."""
        # Pack freelist data in a single call (O(n) instead of O(n²)) via string concat
        count = len(self.free_page_ids)
        freelist_data = struct.pack(f"<I{count}I", count, *self.free_page_ids)

        # Create header with placeholder checksum
        header = PageHeader(page_type=PageType.FREELIST, page_id=self.page_id, checksum=0)
        header_bytes = header.to_bytes()

        # Assemble full page first (with padding)
        full_page = (header_bytes + freelist_data).ljust(page_size, b"\x00")

        # Compute checksum over entire page
        checksum = compute_checksum(full_page)

        # Repack with actual checksum
        header = PageHeader(page_type=PageType.FREELIST, page_id=self.page_id, checksum=checksum)
        header_bytes = header.to_bytes()

        return header_bytes + full_page[PageHeader.SIZE :]

    @classmethod
    def from_bytes(cls, data: bytes, verify_checksum: bool = True) -> Self:
        """Deserialize from bytes."""
        header = PageHeader.from_bytes(data)

        if header.page_type != PageType.FREELIST:
            raise ValueError(f"Expected FREELIST page, got {header.page_type}")

        offset = PageHeader.SIZE
        if len(data) < offset + 4:
            raise ValueError("Data too short for freelist page")

        (count,) = struct.unpack(FREELIST_COUNT_FMT, data[offset : offset + 4])
        offset += 4

        free_page_ids = []
        for _ in range(count):
            if len(data) < offset + 4:
                raise ValueError("Data too short for freelist entries")
            (pid,) = struct.unpack("<I", data[offset : offset + 4])
            free_page_ids.append(pid)
            offset += 4

        if verify_checksum:
            # Compute checksum over entire page (with zero checksum in header)
            header_with_zero = PageHeader(page_type=PageType.FREELIST, page_id=header.page_id, checksum=0)
            check_data = header_with_zero.to_bytes() + data[PageHeader.SIZE :]
            expected = compute_checksum(check_data)
            if header.checksum != expected:
                raise ValueError(f"Checksum mismatch: expected {expected}, got {header.checksum}")

        return cls(page_id=header.page_id, free_page_ids=free_page_ids)

    def max_entries(self, page_size: int = DEFAULT_PAGE_SIZE) -> int:
        """Maximum number of page IDs that fit in this page."""
        available = page_size - PageHeader.SIZE - 4  # header + count
        return available // 4


class LeafPage(BaseModel):
    """B+ tree leaf page containing key-value pairs.

    Cells are stored from the end of the page backward, while
    cell offsets are stored after the header growing forward.
    This allows efficient insertions without moving data.

    Layout:
        [PageHeader:9][cell_count:2][right_sibling:4][cell_offsets:2*n]...[cells]
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    page_id: int
    right_sibling: int = 0  # Page ID of right sibling (0 = none)
    cells: list[tuple[bytes, bytes]] = []  # (key, value) pairs

    def to_bytes(self, page_size: int = DEFAULT_PAGE_SIZE) -> bytes:
        """Serialize to a full page with header and checksum."""
        cell_count = len(self.cells)

        # Calculate cell data and offsets (O(n) using append + reverse)
        cell_parts: list[bytes] = []
        cell_offsets: list[int] = []

        # Cells grow from end of page backward
        cell_area_start = page_size

        for key, value in reversed(self.cells):
            # Cell format: [key_len:2][value_len:2][key][value]
            cell = struct.pack("<HH", len(key), len(value)) + key + value
            cell_area_start -= len(cell)
            cell_offsets.append(cell_area_start)
            cell_parts.append(cell)

        # Reverse to get correct order
        cell_offsets.reverse()
        cell_parts.reverse()
        cell_data = b"".join(cell_parts)

        # Build page content
        leaf_header = struct.pack(LEAF_HEADER_FMT, cell_count, self.right_sibling)

        # Cell offsets (2 bytes each)
        offsets_data = b"".join(struct.pack("<H", off) for off in cell_offsets)

        # Create header with placeholder checksum
        header = PageHeader(page_type=PageType.LEAF, page_id=self.page_id, checksum=0)
        header_bytes = header.to_bytes()

        # Assemble page without checksum first
        front_data = header_bytes + leaf_header + offsets_data
        padding_size = cell_area_start - len(front_data)

        if padding_size < 0:
            raise ValueError(
                f"Page overflow: cells require {len(front_data) + len(cell_data)} bytes, but page size is {page_size}"
            )

        full_page = front_data + (b"\x00" * padding_size) + cell_data

        # Compute checksum over entire page (with zero checksum in header)
        checksum = compute_checksum(full_page)

        # Repack header with actual checksum
        header = PageHeader(page_type=PageType.LEAF, page_id=self.page_id, checksum=checksum)
        header_bytes = header.to_bytes()

        # Replace header in full page
        return header_bytes + full_page[PageHeader.SIZE :]

    def available_space(self, page_size: int = DEFAULT_PAGE_SIZE) -> int:
        """Calculate available space for new cells in bytes.

        Returns the number of bytes available for additional cell data.
        Each new cell requires: 2 (offset) + 4 (key_len + value_len) + len(key) + len(value)
        """
        # Fixed overhead: PageHeader + leaf_header (cell_count + right_sibling)
        fixed_overhead = PageHeader.SIZE + LEAF_HEADER_SIZE

        # Current cell offsets: 2 bytes each
        offsets_size = 2 * len(self.cells)

        # Current cell data size
        cells_size = sum(4 + len(key) + len(value) for key, value in self.cells)

        used = fixed_overhead + offsets_size + cells_size
        return max(0, page_size - used)

    @classmethod
    def from_bytes(cls, data: bytes, verify_checksum: bool = True) -> Self:
        """Deserialize from bytes."""
        header = PageHeader.from_bytes(data)

        if header.page_type != PageType.LEAF:
            raise ValueError(f"Expected LEAF page, got {header.page_type}")

        offset = PageHeader.SIZE
        cell_count, right_sibling = struct.unpack(LEAF_HEADER_FMT, data[offset : offset + LEAF_HEADER_SIZE])
        offset += LEAF_HEADER_SIZE

        # Read cell offsets
        cell_offsets = []
        for _ in range(cell_count):
            (off,) = struct.unpack("<H", data[offset : offset + 2])
            cell_offsets.append(off)
            offset += 2

        # Read cells
        cells = []
        for cell_offset in cell_offsets:
            key_len, value_len = struct.unpack("<HH", data[cell_offset : cell_offset + 4])
            key = data[cell_offset + 4 : cell_offset + 4 + key_len]
            value = data[cell_offset + 4 + key_len : cell_offset + 4 + key_len + value_len]
            cells.append((key, value))

        if verify_checksum:
            # Compute checksum over entire page content (with zero checksum in header)
            header_with_zero = PageHeader(page_type=PageType.LEAF, page_id=header.page_id, checksum=0)
            check_data = header_with_zero.to_bytes() + data[PageHeader.SIZE :]
            expected = compute_checksum(check_data)
            if header.checksum != expected:
                raise ValueError(f"Checksum mismatch: expected {expected}, got {header.checksum}")

        return cls(page_id=header.page_id, right_sibling=right_sibling, cells=cells)


class BranchPage(BaseModel):
    """B+ tree branch (internal) page containing separator keys and child pointers.

    Layout:
        [PageHeader:9][key_count:2][child_0:4][key_0][child_1:4][key_1]...[child_n:4]

    For n keys, there are n+1 children. Keys[i] is the separator between
    children[i] and children[i+1].
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    page_id: int
    keys: list[bytes] = []  # Separator keys
    children: list[int] = []  # Child page IDs (len = len(keys) + 1)

    @model_validator(mode="after")
    def validate_children_keys_invariant(self) -> Self:
        """Ensure len(children) == len(keys) + 1 for non-empty pages."""
        if not self.keys and not self.children:
            return self
        if len(self.children) != len(self.keys) + 1:
            raise ValueError(
                f"BranchPage invariant violated: len(children)={len(self.children)} "
                f"must equal len(keys)+1={len(self.keys) + 1}"
            )
        return self

    def to_bytes(self, page_size: int = DEFAULT_PAGE_SIZE) -> bytes:
        """Serialize to a full page with header and checksum."""
        key_count = len(self.keys)

        # Build branch data: key_count + interleaved children and keys
        branch_data = struct.pack(BRANCH_HEADER_FMT, key_count)

        # First child
        if self.children:
            branch_data += struct.pack("<I", self.children[0])

        # Interleaved keys and remaining children
        for i, key in enumerate(self.keys):
            # Key: [key_len:2][key_data]
            branch_data += struct.pack("<H", len(key)) + key
            if i + 1 < len(self.children):
                branch_data += struct.pack("<I", self.children[i + 1])

        # Create header with placeholder checksum
        header = PageHeader(page_type=PageType.BRANCH, page_id=self.page_id, checksum=0)
        header_bytes = header.to_bytes()

        content_size = len(header_bytes) + len(branch_data)
        if content_size > page_size:
            raise ValueError(f"Page overflow: branch data requires {content_size} bytes, but page size is {page_size}")

        # Assemble full page first
        full_page = (header_bytes + branch_data).ljust(page_size, b"\x00")

        # Compute checksum over entire page
        checksum = compute_checksum(full_page)

        # Repack with actual checksum
        header = PageHeader(page_type=PageType.BRANCH, page_id=self.page_id, checksum=checksum)
        header_bytes = header.to_bytes()

        return header_bytes + full_page[PageHeader.SIZE :]

    def available_space(self, page_size: int = DEFAULT_PAGE_SIZE) -> int:
        """Calculate available space for new keys in bytes.

        Returns the number of bytes available for additional keys/children.
        Each new key requires: 2 (key_len) + len(key) + 4 (child pointer)
        """
        # Fixed overhead: PageHeader + key_count + first child
        fixed_overhead = PageHeader.SIZE + BRANCH_HEADER_SIZE + (4 if self.children else 0)

        # Current keys and children (excluding first child)
        keys_size = sum(2 + len(key) + 4 for key in self.keys)

        used = fixed_overhead + keys_size
        return max(0, page_size - used)

    @classmethod
    def from_bytes(cls, data: bytes, verify_checksum: bool = True) -> Self:
        """Deserialize from bytes."""
        header = PageHeader.from_bytes(data)

        if header.page_type != PageType.BRANCH:
            raise ValueError(f"Expected BRANCH page, got {header.page_type}")

        offset = PageHeader.SIZE
        (key_count,) = struct.unpack(BRANCH_HEADER_FMT, data[offset : offset + BRANCH_HEADER_SIZE])
        offset += BRANCH_HEADER_SIZE

        children = []
        keys = []

        # First child
        if key_count >= 0:
            (child,) = struct.unpack("<I", data[offset : offset + 4])
            children.append(child)
            offset += 4

        # Interleaved keys and children
        for _ in range(key_count):
            (key_len,) = struct.unpack("<H", data[offset : offset + 2])
            offset += 2
            key = data[offset : offset + key_len]
            keys.append(key)
            offset += key_len

            (child,) = struct.unpack("<I", data[offset : offset + 4])
            children.append(child)
            offset += 4

        if verify_checksum:
            # Compute checksum over entire page (with zero checksum in header)
            header_with_zero = PageHeader(page_type=PageType.BRANCH, page_id=header.page_id, checksum=0)
            check_data = header_with_zero.to_bytes() + data[PageHeader.SIZE :]
            expected = compute_checksum(check_data)
            if header.checksum != expected:
                raise ValueError(f"Checksum mismatch: expected {expected}, got {header.checksum}")

        return cls(page_id=header.page_id, keys=keys, children=children)
