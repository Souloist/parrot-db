"""Unit tests for parrot_db models."""

import pytest
from pydantic import ValidationError

from models import MAGIC, DBMetadata, KeyValue, PageHeader, PageType, WALEntry, WALOperation


class TestKeyValue:
    """Tests for KeyValue model."""

    def test_round_trip_basic(self):
        """Test basic serialization round-trip."""
        kv = KeyValue(key=b"foo", value=b"bar", version=1)
        data = kv.to_bytes()
        restored = KeyValue.from_bytes(data)

        assert restored.key == kv.key
        assert restored.value == kv.value
        assert restored.version == kv.version

    def test_round_trip_empty_value(self):
        """Test round-trip with empty value."""
        kv = KeyValue(key=b"key", value=b"", version=42)
        data = kv.to_bytes()
        restored = KeyValue.from_bytes(data)

        assert restored.key == b"key"
        assert restored.value == b""
        assert restored.version == 42

    def test_round_trip_empty_key(self):
        """Test round-trip with empty key."""
        kv = KeyValue(key=b"", value=b"value", version=1)
        data = kv.to_bytes()
        restored = KeyValue.from_bytes(data)

        assert restored.key == b""
        assert restored.value == b"value"

    def test_round_trip_large_values(self):
        """Test round-trip with large key and value."""
        large_key = b"k" * 10000
        large_value = b"v" * 100000
        kv = KeyValue(key=large_key, value=large_value, version=999999)
        data = kv.to_bytes()
        restored = KeyValue.from_bytes(data)

        assert restored.key == large_key
        assert restored.value == large_value
        assert restored.version == 999999

    def test_round_trip_binary_data(self):
        """Test round-trip with binary data including null bytes."""
        key = b"\x00\x01\x02\xff\xfe"
        value = b"\x00" * 100 + b"\xff" * 100
        kv = KeyValue(key=key, value=value, version=1)
        data = kv.to_bytes()
        restored = KeyValue.from_bytes(data)

        assert restored.key == key
        assert restored.value == value

    def test_round_trip_unicode_as_bytes(self):
        """Test round-trip with unicode characters encoded as bytes."""
        key = "héllo".encode("utf-8")
        value = "世界".encode("utf-8")
        kv = KeyValue(key=key, value=value, version=1)
        data = kv.to_bytes()
        restored = KeyValue.from_bytes(data)

        assert restored.key == key
        assert restored.value == value

    def test_string_auto_conversion(self):
        """Test that strings are automatically converted to bytes."""
        kv = KeyValue(key="string_key", value="string_value", version=1)
        assert kv.key == b"string_key"
        assert kv.value == b"string_value"

    def test_from_bytes_too_short(self):
        """Test that from_bytes rejects data that's too short."""
        with pytest.raises(ValueError, match="Data too short"):
            KeyValue.from_bytes(b"\x00" * 10)

    def test_from_bytes_truncated_payload(self):
        """Test that from_bytes rejects truncated payload."""
        kv = KeyValue(key=b"key", value=b"value", version=1)
        data = kv.to_bytes()
        with pytest.raises(ValueError, match="Data too short"):
            KeyValue.from_bytes(data[:-1])


class TestWALEntry:
    """Tests for WALEntry model."""

    def test_round_trip_put(self):
        """Test round-trip for PUT operation."""
        entry = WALEntry(op=WALOperation.PUT, key=b"foo", value=b"bar", txn_id=123, timestamp=1234567890.5)
        data = entry.to_bytes()
        restored = WALEntry.from_bytes(data)

        assert restored.op == WALOperation.PUT
        assert restored.key == b"foo"
        assert restored.value == b"bar"
        assert restored.txn_id == 123
        assert restored.timestamp == pytest.approx(1234567890.5)

    def test_round_trip_delete(self):
        """Test round-trip for DELETE operation with empty value."""
        entry = WALEntry(op=WALOperation.DELETE, key=b"deleted_key", value=b"", txn_id=456, timestamp=1234567890.0)
        data = entry.to_bytes()
        restored = WALEntry.from_bytes(data)

        assert restored.op == WALOperation.DELETE
        assert restored.key == b"deleted_key"
        assert restored.value == b""
        assert restored.txn_id == 456

    def test_round_trip_commit(self):
        """Test round-trip for COMMIT operation."""
        entry = WALEntry(op=WALOperation.COMMIT, key=b"", value=b"", txn_id=789, timestamp=1234567890.0)
        data = entry.to_bytes()
        restored = WALEntry.from_bytes(data)

        assert restored.op == WALOperation.COMMIT
        assert restored.txn_id == 789

    def test_round_trip_rollback(self):
        """Test round-trip for ROLLBACK operation."""
        entry = WALEntry(op=WALOperation.ROLLBACK, key=b"", value=b"", txn_id=999, timestamp=1234567890.0)
        data = entry.to_bytes()
        restored = WALEntry.from_bytes(data)

        assert restored.op == WALOperation.ROLLBACK
        assert restored.txn_id == 999

    def test_create_factory(self):
        """Test the create factory method auto-generates timestamp."""
        entry = WALEntry.create(op=WALOperation.PUT, key=b"key", value=b"value", txn_id=1)
        assert entry.timestamp > 0
        assert entry.op == WALOperation.PUT

    def test_round_trip_large_values(self):
        """Test round-trip with large key and value."""
        large_key = b"k" * 10000
        large_value = b"v" * 100000
        entry = WALEntry(op=WALOperation.PUT, key=large_key, value=large_value, txn_id=1, timestamp=1.0)
        data = entry.to_bytes()
        restored = WALEntry.from_bytes(data)

        assert restored.key == large_key
        assert restored.value == large_value

    def test_from_bytes_too_short(self):
        """Test that from_bytes rejects data that's too short."""
        with pytest.raises(ValueError, match="Data too short"):
            WALEntry.from_bytes(b"\x00" * 10)


class TestPageHeader:
    """Tests for PageHeader model."""

    def test_round_trip_meta(self):
        """Test round-trip for META page type."""
        header = PageHeader(page_type=PageType.META, page_id=0, checksum=0xDEADBEEF)
        data = header.to_bytes()
        restored = PageHeader.from_bytes(data)

        assert restored.page_type == PageType.META
        assert restored.page_id == 0
        assert restored.checksum == 0xDEADBEEF

    def test_round_trip_leaf(self):
        """Test round-trip for LEAF page type."""
        header = PageHeader(page_type=PageType.LEAF, page_id=42, checksum=12345)
        data = header.to_bytes()
        restored = PageHeader.from_bytes(data)

        assert restored.page_type == PageType.LEAF
        assert restored.page_id == 42
        assert restored.checksum == 12345

    def test_round_trip_branch(self):
        """Test round-trip for BRANCH page type."""
        header = PageHeader(page_type=PageType.BRANCH, page_id=100, checksum=0)
        data = header.to_bytes()
        restored = PageHeader.from_bytes(data)

        assert restored.page_type == PageType.BRANCH
        assert restored.page_id == 100

    def test_round_trip_freelist(self):
        """Test round-trip for FREELIST page type."""
        header = PageHeader(page_type=PageType.FREELIST, page_id=1, checksum=0xFFFFFFFF)
        data = header.to_bytes()
        restored = PageHeader.from_bytes(data)

        assert restored.page_type == PageType.FREELIST
        assert restored.page_id == 1
        assert restored.checksum == 0xFFFFFFFF

    def test_header_size(self):
        """Test that header serializes to expected size."""
        header = PageHeader(page_type=PageType.META, page_id=0, checksum=0)
        assert len(header.to_bytes()) == PageHeader.SIZE
        assert PageHeader.SIZE == 9

    def test_from_bytes_invalid_page_type(self):
        """Test that from_bytes rejects invalid page type."""
        # Create data with invalid page type (99)
        data = b"\x63" + b"\x00" * 8  # 99 in first byte
        with pytest.raises(ValueError, match="Invalid page type"):
            PageHeader.from_bytes(data)

    def test_from_bytes_too_short(self):
        """Test that from_bytes rejects data that's too short."""
        with pytest.raises(ValueError, match="Data too short"):
            PageHeader.from_bytes(b"\x00" * 5)


class TestDBMetadata:
    """Tests for DBMetadata model."""

    def test_round_trip_defaults(self):
        """Test round-trip with default values."""
        meta = DBMetadata()
        data = meta.to_bytes()
        restored = DBMetadata.from_bytes(data)

        assert restored.version == DBMetadata.FORMAT_VERSION
        assert restored.page_size == 4096
        assert restored.root_page_id == 0
        assert restored.freelist_page_id == 0
        assert restored.txn_id == 0

    def test_round_trip_custom_values(self):
        """Test round-trip with custom values."""
        meta = DBMetadata(page_size=8192, root_page_id=10, freelist_page_id=5, txn_id=1000)
        data = meta.to_bytes()
        restored = DBMetadata.from_bytes(data)

        assert restored.page_size == 8192
        assert restored.root_page_id == 10
        assert restored.freelist_page_id == 5
        assert restored.txn_id == 1000

    def test_metadata_size(self):
        """Test that metadata serializes to expected size."""
        meta = DBMetadata()
        assert len(meta.to_bytes()) == DBMetadata.SIZE
        assert DBMetadata.SIZE == 32

    def test_magic_bytes(self):
        """Test that magic bytes are correct."""
        meta = DBMetadata()
        data = meta.to_bytes()
        assert data[:4] == MAGIC
        assert data[:4] == b"PRRT"

    def test_from_bytes_invalid_magic(self):
        """Test that from_bytes rejects invalid magic bytes."""
        data = b"XXXX" + b"\x00" * 28
        with pytest.raises(ValueError, match="Invalid magic bytes"):
            DBMetadata.from_bytes(data)

    def test_from_bytes_too_short(self):
        """Test that from_bytes rejects data that's too short."""
        with pytest.raises(ValueError, match="Data too short"):
            DBMetadata.from_bytes(b"PRRT" + b"\x00" * 10)

    def test_page_size_validation_too_small(self):
        """Test that page_size below minimum is rejected."""
        with pytest.raises(ValidationError):
            DBMetadata(page_size=256)

    def test_page_size_validation_too_large(self):
        """Test that page_size above maximum is rejected."""
        with pytest.raises(ValidationError):
            DBMetadata(page_size=131072)

    def test_page_size_validation_not_power_of_two(self):
        """Test that non-power-of-two page_size is rejected."""
        with pytest.raises(ValidationError):
            DBMetadata(page_size=1000)

    def test_page_size_valid_powers_of_two(self):
        """Test valid power-of-two page sizes."""
        for size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]:
            meta = DBMetadata(page_size=size)
            assert meta.page_size == size


class TestEnums:
    """Tests for enum types."""

    def test_page_type_values(self):
        """Test PageType enum values match spec."""
        assert PageType.META == 1
        assert PageType.FREELIST == 2
        assert PageType.BRANCH == 3
        assert PageType.LEAF == 4

    def test_wal_operation_values(self):
        """Test WALOperation enum values."""
        assert WALOperation.PUT == 1
        assert WALOperation.DELETE == 2
        assert WALOperation.COMMIT == 3
        assert WALOperation.ROLLBACK == 4
