"""Tests for transaction layer with snapshot isolation."""

from pathlib import Path

import pytest

from parrot_db import ParrotDB


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> ParrotDB:
    """Create a fresh database for testing."""
    with ParrotDB(db_path, create=True) as database:
        yield database


class TestBasicTransactions:
    """Test basic transaction operations."""

    def test_read_transaction_sees_committed_data(self, db: ParrotDB):
        """Read transaction can see data committed before it started."""
        # Write some data
        db.put(b"key1", b"value1")
        db.put(b"key2", b"value2")

        # Start read transaction and verify data is visible
        with db.begin() as txn:
            assert txn.get(b"key1") == b"value1"
            assert txn.get(b"key2") == b"value2"
            assert txn.get(b"nonexistent") is None

    def test_write_transaction_commit(self, db: ParrotDB):
        """Write transaction commits data atomically."""
        with db.begin(write=True) as txn:
            txn.put(b"key1", b"value1")
            txn.put(b"key2", b"value2")
            txn.commit()

        # Verify data is visible after commit
        assert db.get(b"key1") == b"value1"
        assert db.get(b"key2") == b"value2"

    def test_write_transaction_rollback(self, db: ParrotDB):
        """Write transaction rollback discards all changes."""
        db.put(b"existing", b"original")

        with db.begin(write=True) as txn:
            txn.put(b"existing", b"modified")
            txn.put(b"new_key", b"new_value")
            txn.rollback()

        # Verify changes were discarded
        assert db.get(b"existing") == b"original"
        assert db.get(b"new_key") is None

    def test_write_transaction_auto_commit(self, db: ParrotDB):
        """Write transaction auto-commits on successful context exit."""
        with db.begin(write=True) as txn:
            txn.put(b"auto", b"committed")
            # No explicit commit - should auto-commit

        assert db.get(b"auto") == b"committed"

    def test_write_transaction_auto_rollback_on_exception(self, db: ParrotDB):
        """Write transaction rolls back on exception."""
        db.put(b"existing", b"original")

        with pytest.raises(ValueError):
            with db.begin(write=True) as txn:
                txn.put(b"existing", b"modified")
                raise ValueError("test error")

        # Verify changes were rolled back
        assert db.get(b"existing") == b"original"

    def test_delete_in_transaction(self, db: ParrotDB):
        """Delete operations work within transactions."""
        db.put(b"to_delete", b"value")

        with db.begin(write=True) as txn:
            assert txn.delete(b"to_delete") is True
            assert txn.delete(b"nonexistent") is False
            txn.commit()

        assert db.get(b"to_delete") is None


class TestSnapshotIsolation:
    """Test snapshot isolation between concurrent transactions."""

    def test_reader_sees_snapshot_at_start_time(self, db: ParrotDB):
        """Reader sees consistent snapshot from transaction start."""
        db.put(b"key", b"initial")

        # Start read transaction
        read_txn = db.begin()
        assert read_txn.get(b"key") == b"initial"

        # Modify data in separate write transaction
        with db.begin(write=True) as write_txn:
            write_txn.put(b"key", b"modified")
            write_txn.put(b"new_key", b"new_value")
            write_txn.commit()

        # Read transaction still sees old data (snapshot isolation)
        assert read_txn.get(b"key") == b"initial"
        assert read_txn.get(b"new_key") is None

        read_txn._close()

        # New read transaction sees new data
        with db.begin() as txn:
            assert txn.get(b"key") == b"modified"
            assert txn.get(b"new_key") == b"new_value"

    def test_multiple_readers_see_same_snapshot(self, db: ParrotDB):
        """Multiple readers starting at same time see same snapshot."""
        db.put(b"key", b"value1")

        # Start multiple read transactions
        txn1 = db.begin()
        txn2 = db.begin()

        assert txn1.get(b"key") == b"value1"
        assert txn2.get(b"key") == b"value1"

        # Modify in write transaction
        with db.begin(write=True) as write_txn:
            write_txn.put(b"key", b"value2")
            write_txn.commit()

        # Both readers still see old value
        assert txn1.get(b"key") == b"value1"
        assert txn2.get(b"key") == b"value1"

        txn1._close()
        txn2._close()

    def test_writer_sees_own_changes(self, db: ParrotDB):
        """Writer can read its own uncommitted changes."""
        db.put(b"key", b"original")

        with db.begin(write=True) as txn:
            txn.put(b"key", b"modified")
            # Writer sees its own change
            assert txn.get(b"key") == b"modified"

            txn.put(b"new", b"value")
            assert txn.get(b"new") == b"value"
            txn.commit()

    def test_range_scan_sees_snapshot(self, db: ParrotDB):
        """Range scan operates on snapshot, not live data."""
        for i in range(10):
            db.put(f"key{i:02d}".encode(), f"value{i}".encode())

        # Start read transaction
        read_txn = db.begin()

        # Modify data
        with db.begin(write=True) as write_txn:
            write_txn.put(b"key00", b"modified")
            write_txn.delete(b"key05")
            write_txn.put(b"key99", b"new")
            write_txn.commit()

        # Range scan sees original snapshot
        results = list(read_txn.range_scan())
        assert len(results) == 10
        assert results[0] == (b"key00", b"value0")  # Not modified
        assert (b"key05", b"value5") in results  # Not deleted
        assert (b"key99", b"new") not in results  # Not added

        read_txn._close()


class TestSingleWriter:
    """Test single writer enforcement."""

    def test_only_one_write_transaction_allowed(self, db: ParrotDB):
        """Only one write transaction can be active at a time."""
        txn1 = db.begin(write=True)

        with pytest.raises(RuntimeError, match="Another write transaction"):
            db.begin(write=True)

        txn1.rollback()

        # Now we can start another write transaction
        txn2 = db.begin(write=True)
        txn2.rollback()

    def test_readers_dont_block_readers(self, db: ParrotDB):
        """Multiple read transactions can run concurrently."""
        txn1 = db.begin()
        txn2 = db.begin()
        txn3 = db.begin()

        # All can read
        txn1.get(b"key")
        txn2.get(b"key")
        txn3.get(b"key")

        txn1._close()
        txn2._close()
        txn3._close()

    def test_write_after_read_commit(self, db: ParrotDB):
        """Write transaction can start while read transactions exist."""
        read_txn = db.begin()

        # Write transaction can start
        with db.begin(write=True) as write_txn:
            write_txn.put(b"key", b"value")
            write_txn.commit()

        read_txn._close()


class TestTransactionAPI:
    """Test transaction API behavior."""

    def test_transaction_context_manager(self, db: ParrotDB):
        """Transactions work as context managers."""
        with db.begin() as txn:
            assert txn.get(b"nonexistent") is None

        with db.begin(write=True) as txn:
            txn.put(b"key", b"value")
            txn.commit()

    def test_closed_transaction_raises(self, db: ParrotDB):
        """Operations on closed transaction raise errors."""
        txn = db.begin()
        txn._close()

        with pytest.raises(RuntimeError, match="no longer active"):
            txn.get(b"key")

    def test_double_commit_raises(self, db: ParrotDB):
        """Committing twice raises error."""
        txn = db.begin(write=True)
        txn.commit()

        with pytest.raises(RuntimeError, match="no longer active"):
            txn.commit()

    def test_commit_after_rollback_raises(self, db: ParrotDB):
        """Commit after rollback raises error."""
        txn = db.begin(write=True)
        txn.rollback()

        with pytest.raises(RuntimeError, match="no longer active"):
            txn.commit()


class TestPersistence:
    """Test data persistence across database opens."""

    def test_data_persists_after_close(self, db_path: Path):
        """Data survives database close and reopen."""
        # Write data and close
        with ParrotDB(db_path, create=True) as db:
            db.put(b"persistent", b"data")

        # Reopen and verify
        with ParrotDB(db_path, create=False) as db:
            assert db.get(b"persistent") == b"data"

    def test_uncommitted_data_not_persisted(self, db_path: Path):
        """Uncommitted data is not visible after reopen."""
        with ParrotDB(db_path, create=True) as db:
            db.put(b"committed", b"yes")

        # Start write but don't commit, then close
        db = ParrotDB(db_path, create=False)
        txn = db.begin(write=True)
        txn.put(b"uncommitted", b"no")
        txn.rollback()
        db.close()

        # Reopen and verify
        with ParrotDB(db_path, create=False) as db:
            assert db.get(b"committed") == b"yes"
            assert db.get(b"uncommitted") is None

    def test_txn_id_increments(self, db_path: Path):
        """Transaction IDs increment across commits."""
        with ParrotDB(db_path, create=True) as db:
            with db.begin(write=True) as txn:
                txn.put(b"key1", b"value1")
                txn_id_1 = txn.txn_id
                txn.commit()

            with db.begin(write=True) as txn:
                txn.put(b"key2", b"value2")
                txn_id_2 = txn.txn_id
                txn.commit()

            assert txn_id_2 > txn_id_1


class TestConvenienceMethods:
    """Test convenience methods on ParrotDB."""

    def test_get_convenience(self, db: ParrotDB):
        """db.get() creates auto read transaction."""
        db.put(b"key", b"value")
        assert db.get(b"key") == b"value"
        assert db.get(b"nonexistent") is None

    def test_put_convenience(self, db: ParrotDB):
        """db.put() creates auto write transaction and commits."""
        db.put(b"key", b"value")
        assert db.get(b"key") == b"value"

    def test_delete_convenience(self, db: ParrotDB):
        """db.delete() creates auto write transaction and commits."""
        db.put(b"key", b"value")
        assert db.delete(b"key") is True
        assert db.get(b"key") is None
        assert db.delete(b"key") is False


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_database(self, db: ParrotDB):
        """Operations work on empty database."""
        assert db.get(b"key") is None

        with db.begin() as txn:
            assert list(txn.range_scan()) == []

    def test_binary_keys_and_values(self, db: ParrotDB):
        """Binary keys and values work correctly."""
        key = bytes(range(256))
        value = b"\x00\xff" * 100

        db.put(key, value)
        assert db.get(key) == value

    def test_empty_key_and_value(self, db: ParrotDB):
        """Empty keys and values are supported."""
        db.put(b"", b"empty_key")
        db.put(b"empty_value", b"")

        assert db.get(b"") == b"empty_key"
        assert db.get(b"empty_value") == b""

    def test_large_transaction(self, db: ParrotDB):
        """Large transactions with many operations work."""
        with db.begin(write=True) as txn:
            for i in range(1000):
                txn.put(f"key{i:04d}".encode(), f"value{i}".encode())
            txn.commit()

        with db.begin() as txn:
            results = list(txn.range_scan())
            assert len(results) == 1000
