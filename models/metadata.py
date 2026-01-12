"""Database metadata model.

Struct format reference (https://docs.python.org/3/library/struct.html):
    <  = little-endian byte order
    4s = 4-byte string (e.g., magic bytes)
    I  = unsigned int (4 bytes)
    Q  = unsigned long long (8 bytes)
"""

import struct
from typing import ClassVar

from pydantic import BaseModel, field_validator

# DBMetadata format: [magic:4][version:4][page_size:4][root_page_id:4][freelist_page_id:4][txn_id:8]
DB_METADATA_FMT = "<4sIIIIQ"
DB_METADATA_HEADER_SIZE = 28  # 4 + 4 + 4 + 4 + 4 + 8
DB_METADATA_SIZE = 32  # Padded size

# Magic bytes to identify parrot-db files
MAGIC = b"PRRT"

# Default page size (4KB)
DEFAULT_PAGE_SIZE = 4096


class DBMetadata(BaseModel):
    """Database-level metadata stored in the file header (page 0).

    This is the first thing read when opening a database file. It validates
    that the file is a parrot-db file and provides locations of key data
    structures (B+ tree root, freelist).

    Layout (32 bytes):
        [magic:4][version:4][page_size:4][root_page_id:4][freelist_page_id:4][txn_id:8][padding:4]

    Fields:
        MAGIC: "PRRT" - identifies this as a parrot-db file
        version: Format version for compatibility checking
        page_size: Bytes per page (must be power of 2, 512-65536)
        root_page_id: Page ID of B+ tree root (0 = empty tree)
        freelist_page_id: Page ID of free page list (0 = none)
        txn_id: Latest committed transaction ID
    """

    # Class constants (see module docstring for format details)
    FORMAT_VERSION: ClassVar[int] = 1
    SIZE: ClassVar[int] = DB_METADATA_SIZE

    version: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    root_page_id: int = 0  # Page ID of the B+ tree root (0 = none)
    freelist_page_id: int = 0  # Page ID of the freelist (0 = none)
    txn_id: int = 0  # Latest committed transaction ID

    @field_validator("page_size")
    @classmethod
    def validate_page_size(cls, v: int) -> int:
        if v < 512 or v > 65536:
            raise ValueError(f"Page size must be between 512 and 65536, got {v}")
        if v & (v - 1) != 0:
            raise ValueError(f"Page size must be a power of 2, got {v}")
        return v

    def to_bytes(self) -> bytes:
        """Serialize to bytes. See module docstring for format details."""
        data = struct.pack(
            DB_METADATA_FMT,
            MAGIC,
            self.version,
            self.page_size,
            self.root_page_id,
            self.freelist_page_id,
            self.txn_id,
        )
        # Pad to SIZE bytes
        return data.ljust(DB_METADATA_SIZE, b"\x00")

    @classmethod
    def from_bytes(cls, data: bytes) -> "DBMetadata":
        """Deserialize from bytes."""
        if len(data) < DB_METADATA_SIZE:
            raise ValueError(f"Data too short: expected at least {DB_METADATA_SIZE} bytes, got {len(data)}")

        magic, version, page_size, root_page_id, freelist_page_id, txn_id = struct.unpack(
            DB_METADATA_FMT, data[:DB_METADATA_HEADER_SIZE]
        )

        if magic != MAGIC:
            raise ValueError(f"Invalid magic bytes: expected {MAGIC!r}, got {magic!r}")

        return cls(
            version=version,
            page_size=page_size,
            root_page_id=root_page_id,
            freelist_page_id=freelist_page_id,
            txn_id=txn_id,
        )
