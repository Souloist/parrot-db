"""Storage-related models: pages and key-value entries.

Struct format reference (https://docs.python.org/3/library/struct.html):
    <  = little-endian byte order
    B  = unsigned char (1 byte)
    I  = unsigned int (4 bytes)
    Q  = unsigned long long (8 bytes)
"""

import struct
from enum import IntEnum
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, field_validator

# KeyValue format: [key_len:4][value_len:4][version:8][key][value]
KEY_VALUE_FMT = "<IIQ"
KEY_VALUE_HEADER_SIZE = 16  # 4 + 4 + 8

# PageHeader format: [page_type:1][page_id:4][checksum:4]
PAGE_HEADER_FMT = "<BII"
PAGE_HEADER_SIZE = 9  # 1 + 4 + 4


class PageType(IntEnum):
    """Page type identifiers for the storage engine."""

    META = 1
    FREELIST = 2
    BRANCH = 3
    LEAF = 4


class KeyValue(BaseModel):
    """A single key-value entry with version metadata.

    Used as the fundamental unit of data storage. The version field enables
    MVCC (Multi-Version Concurrency Control) - multiple versions of the same
    key can exist simultaneously for snapshot isolation.
    """

    # arbitrary_types_allowed permits bytes fields without Pydantic coercion
    model_config = ConfigDict(arbitrary_types_allowed=True)

    key: bytes
    value: bytes
    version: int

    @field_validator("key", "value", mode="before")
    @classmethod
    def ensure_bytes(cls, v: bytes | str) -> bytes:
        if isinstance(v, str):
            return v.encode("utf-8")
        return v

    def to_bytes(self) -> bytes:
        """Serialize to bytes. See module docstring for format details."""
        header = struct.pack(KEY_VALUE_FMT, len(self.key), len(self.value), self.version)
        return header + self.key + self.value

    @classmethod
    def from_bytes(cls, data: bytes) -> "KeyValue":
        """Deserialize from bytes."""
        if len(data) < KEY_VALUE_HEADER_SIZE:
            raise ValueError(f"Data too short: expected at least {KEY_VALUE_HEADER_SIZE} bytes, got {len(data)}")

        key_len, value_len, version = struct.unpack(KEY_VALUE_FMT, data[:KEY_VALUE_HEADER_SIZE])

        expected_len = KEY_VALUE_HEADER_SIZE + key_len + value_len
        if len(data) < expected_len:
            raise ValueError(f"Data too short: expected {expected_len} bytes, got {len(data)}")

        key = data[KEY_VALUE_HEADER_SIZE : KEY_VALUE_HEADER_SIZE + key_len]
        value = data[KEY_VALUE_HEADER_SIZE + key_len : KEY_VALUE_HEADER_SIZE + key_len + value_len]

        return cls(key=key, value=value, version=version)


class PageHeader(BaseModel):
    """Metadata for a storage page.

    Every fixed-size page (default 4KB) starts with this header. It identifies
    the page type and provides a checksum for corruption detection.

    Layout (9 bytes):
        offset  size  field
        ------  ----  -----
        0       1     page_type
        1       4     page_id
        5       4     checksum (CRC32)
    """

    # Header size in bytes
    SIZE: ClassVar[int] = PAGE_HEADER_SIZE

    page_type: PageType
    page_id: int
    checksum: int = 0  # CRC32, computed over page content

    def to_bytes(self) -> bytes:
        """Serialize to bytes. See module docstring for format details."""
        return struct.pack(PAGE_HEADER_FMT, self.page_type, self.page_id, self.checksum)

    @classmethod
    def from_bytes(cls, data: bytes) -> "PageHeader":
        """Deserialize from bytes."""
        if len(data) < PAGE_HEADER_SIZE:
            raise ValueError(f"Data too short: expected at least {PAGE_HEADER_SIZE} bytes, got {len(data)}")

        page_type, page_id, checksum = struct.unpack(PAGE_HEADER_FMT, data[:PAGE_HEADER_SIZE])

        if page_type not in PageType._value2member_map_:
            raise ValueError(f"Invalid page type: {page_type}")

        return cls(page_type=PageType(page_type), page_id=page_id, checksum=checksum)
