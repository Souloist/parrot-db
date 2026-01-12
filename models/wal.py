"""Write-Ahead Log (WAL) models.

Struct format reference (https://docs.python.org/3/library/struct.html):
    <  = little-endian byte order
    B  = unsigned char (1 byte)
    I  = unsigned int (4 bytes)
    Q  = unsigned long long (8 bytes)
    d  = double float (8 bytes)
"""

import struct
import time
from enum import IntEnum

from pydantic import BaseModel, ConfigDict, field_validator

# WALEntry format: [op:1][key_len:4][value_len:4][txn_id:8][timestamp:8][key][value]
WAL_ENTRY_FMT = "<BIIQd"
WAL_ENTRY_HEADER_SIZE = 25  # 1 + 4 + 4 + 8 + 8


class WALOperation(IntEnum):
    """WAL operation types."""

    PUT = 1
    DELETE = 2
    COMMIT = 3
    ROLLBACK = 4


class WALEntry(BaseModel):
    """Represents a single WAL record.

    The Write-Ahead Log allows for crash/transaction recovery. Before any change hits
    the main data store, it's logged here first. On crash recovery, we replay
    the WAL to restore committed transactions.

    Fields:
        op: Operation type (PUT, DELETE, COMMIT, ROLLBACK)
        key: The key being modified
        value: New value (empty for DELETE/COMMIT/ROLLBACK)
        txn_id: Transaction identifier
        timestamp: Unix timestamp when the entry was created
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    op: WALOperation
    key: bytes
    value: bytes  # Empty for DELETE operations
    txn_id: int
    timestamp: float  # Unix timestamp

    @field_validator("key", "value", mode="before")
    @classmethod
    def ensure_bytes(cls, v: bytes | str) -> bytes:
        if isinstance(v, str):
            return v.encode("utf-8")
        return v

    @classmethod
    def create(cls, op: WALOperation, key: bytes, value: bytes, txn_id: int) -> "WALEntry":
        """Factory method that auto-generates timestamp."""
        return cls(op=op, key=key, value=value, txn_id=txn_id, timestamp=time.time())

    def to_bytes(self) -> bytes:
        """Serialize to bytes. See module docstring for format details."""
        header = struct.pack(WAL_ENTRY_FMT, self.op, len(self.key), len(self.value), self.txn_id, self.timestamp)
        return header + self.key + self.value

    @classmethod
    def from_bytes(cls, data: bytes) -> "WALEntry":
        """Deserialize from bytes."""
        if len(data) < WAL_ENTRY_HEADER_SIZE:
            raise ValueError(f"Data too short: expected at least {WAL_ENTRY_HEADER_SIZE} bytes, got {len(data)}")

        op, key_len, value_len, txn_id, timestamp = struct.unpack(WAL_ENTRY_FMT, data[:WAL_ENTRY_HEADER_SIZE])

        expected_len = WAL_ENTRY_HEADER_SIZE + key_len + value_len
        if len(data) < expected_len:
            raise ValueError(f"Data too short: expected {expected_len} bytes, got {len(data)}")

        key = data[WAL_ENTRY_HEADER_SIZE : WAL_ENTRY_HEADER_SIZE + key_len]
        value = data[WAL_ENTRY_HEADER_SIZE + key_len : WAL_ENTRY_HEADER_SIZE + key_len + value_len]

        return cls(op=WALOperation(op), key=key, value=value, txn_id=txn_id, timestamp=timestamp)
