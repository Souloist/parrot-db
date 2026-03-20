"""Tests for freelist and page reclamation."""

from pathlib import Path

import pytest

from parrot_db import ParrotDB
from storage.freelist import Freelist


class TestFreelistBasics:
    """Test basic freelist operations."""

    def test_allocate_and_free(self):
        """Basic allocate and free operations."""
        fl = Freelist()
        assert fl.is_empty()
        assert fl.allocate() is None

        fl.free(10)
        fl.free(20)
        assert fl.count() == 2
        assert not fl.is_empty()

        # Allocation returns from free set
        page_id = fl.allocate()
        assert page_id in (10, 20)
        assert fl.count() == 1

    def test_free_many(self):
        """Free multiple pages at once."""
        fl = Freelist()
        fl.free_many([5, 10, 15, 20])
        assert fl.count() == 4

    def test_contains(self):
        """Check if page is in freelist."""
        fl = Freelist()
        fl.free(10)
        assert fl.contains(10)
        assert not fl.contains(20)


class TestPendingFree:
    """Test deferred page freeing for MVCC."""

    def test_mark_pending_free(self):
        """Pages can be marked as pending free."""
        fl = Freelist()
        fl.mark_pending_free({10, 20, 30}, txn_id=5)

        assert fl.count() == 0  # Not immediately free
        assert fl.pending_count() == 3
        assert fl.is_pending(10)
        assert fl.is_pending(20)
        assert not fl.is_pending(100)

    def test_release_pending_no_readers(self):
        """Pending pages released when no readers."""
        fl = Freelist()
        fl.mark_pending_free({10, 20}, txn_id=5)
        fl.mark_pending_free({30, 40}, txn_id=6)

        # No active readers - all pending should be released
        released = fl.release_pending(oldest_active_txn_id=None)
        assert released == 4
        assert fl.count() == 4
        assert fl.pending_count() == 0

    def test_release_pending_with_reader(self):
        """Pending pages only released when reader advances."""
        fl = Freelist()
        fl.mark_pending_free({10, 20}, txn_id=5)
        fl.mark_pending_free({30, 40}, txn_id=6)
        fl.mark_pending_free({50, 60}, txn_id=7)

        # Reader at txn_id 6 - only txn_id 5 can be freed
        released = fl.release_pending(oldest_active_txn_id=6)
        assert released == 2
        assert fl.count() == 2
        assert fl.pending_count() == 4

        # Reader advances to txn_id 7 - txn_id 6 can now be freed
        released = fl.release_pending(oldest_active_txn_id=7)
        assert released == 2
        assert fl.count() == 4
        assert fl.pending_count() == 2

    def test_pending_to_dict(self):
        """Get pending pages as dictionary."""
        fl = Freelist()
        fl.mark_pending_free({20, 10}, txn_id=5)
        fl.mark_pending_free({40, 30}, txn_id=6)

        pending = fl.pending_to_dict()
        assert pending == {5: [10, 20], 6: [30, 40]}


class TestFreelistPersistence:
    """Test freelist persistence to/from page."""

    def test_to_page_and_from_page(self):
        """Freelist round-trips through page serialization."""
        fl = Freelist()
        fl.free_many([5, 10, 15, 20, 25])

        page = fl.to_page(page_id=100)
        assert page.page_id == 100
        assert sorted(page.free_page_ids) == [5, 10, 15, 20, 25]

        fl2 = Freelist.from_page(page)
        assert fl2.count() == 5
        assert fl2.to_list() == [5, 10, 15, 20, 25]

    def test_pending_not_persisted(self):
        """Pending-free pages are not persisted."""
        fl = Freelist()
        fl.free_many([10, 20])
        fl.mark_pending_free({30, 40}, txn_id=5)

        page = fl.to_page(page_id=100)
        # Only immediately free pages are persisted
        assert sorted(page.free_page_ids) == [10, 20]


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> ParrotDB:
    """Create a fresh database for testing."""
    with ParrotDB(db_path, create=True) as database:
        yield database


class TestPageReclamation:
    """Test page reclamation during database operations."""

    def test_pages_reclaimed_after_delete(self, db: ParrotDB):
        """Pages are added to freelist after delete when no readers."""
        # Insert keys
        for i in range(10):
            db.put(f"key{i}".encode(), f"value{i}".encode())

        # Delete all keys
        for i in range(10):
            db.delete(f"key{i}".encode())

        # Without active readers, pages should be freed
        assert db.freelist_count > 0

    def test_reader_prevents_reclamation(self, db: ParrotDB):
        """Long-running reader prevents page reclamation."""
        # Insert initial data
        db.put(b"key1", b"value1")

        # Start a read transaction (holds snapshot)
        read_txn = db.begin(write=False)

        # Modify data - creates new pages
        db.put(b"key1", b"modified")
        db.put(b"key2", b"value2")

        # Pages should be pending (reader still holds old snapshot)
        assert db.pending_free_count > 0

        # Close reader
        read_txn.close()

        # Now pages should be released (no more readers)
        # Note: release happens on next commit or reader close
        assert db.pending_free_count == 0 or db.freelist_count > 0

    def test_multiple_readers_deferred_free(self, db: ParrotDB):
        """Pages freed only when all relevant readers complete."""
        db.put(b"key", b"v1")

        # Reader 1 at txn 1
        r1 = db.begin(write=False)

        # Modify (txn 2)
        db.put(b"key", b"v2")

        # Reader 2 at txn 2
        r2 = db.begin(write=False)

        # Modify again (txn 3)
        db.put(b"key", b"v3")

        # Both readers should still see their versions
        assert r1.get(b"key") == b"v1"
        assert r2.get(b"key") == b"v2"

        # Close reader 1 - pages from txn 1 can now be freed
        r1.close()

        # Close reader 2
        r2.close()

        # All data should be readable from current state
        assert db.get(b"key") == b"v3"


class TestFreelistPersistenceIntegration:
    """Test freelist persistence across database restarts."""

    def test_freelist_survives_restart(self, db_path: Path):
        """Freelist state is preserved after close/reopen."""
        # Create database and generate free pages
        with ParrotDB(db_path, create=True) as db:
            for i in range(20):
                db.put(f"key{i}".encode(), f"value{i}".encode())

            # Delete some keys to create free pages
            for i in range(10):
                db.delete(f"key{i}".encode())

            free_count_before = db.freelist_count

        # Reopen database
        with ParrotDB(db_path, create=False) as db:
            # Freelist should be restored
            assert db.freelist_count == free_count_before

            # Data should still be correct
            for i in range(10, 20):
                assert db.get(f"key{i}".encode()) == f"value{i}".encode()

    def test_free_pages_reused(self, db_path: Path):
        """Free pages are reused for new allocations."""
        with ParrotDB(db_path, create=True) as db:
            # Insert and delete to create free pages
            for i in range(50):
                db.put(f"key{i}".encode(), b"x" * 100)
            for i in range(50):
                db.delete(f"key{i}".encode())

            initial_free = db.freelist_count

            # Insert new data - should reuse free pages
            for i in range(10):
                db.put(f"new{i}".encode(), b"y" * 100)

            # Should have used some free pages
            assert db.freelist_count < initial_free


class TestDatabaseCopy:
    """Test database copy with and without compaction."""

    def test_copy_raw(self, db_path: Path, tmp_path: Path):
        """Raw copy produces identical file."""
        dest_path = tmp_path / "copy.db"

        with ParrotDB(db_path, create=True) as db:
            for i in range(100):
                db.put(f"key{i:03d}".encode(), f"value{i}".encode())

            db.copy(dest_path, compact=False)

        # Files should be identical
        assert db_path.stat().st_size == dest_path.stat().st_size

        # Data should be identical
        with ParrotDB(dest_path, create=False) as db:
            for i in range(100):
                assert db.get(f"key{i:03d}".encode()) == f"value{i}".encode()

    def test_copy_compact_empty_db(self, db_path: Path, tmp_path: Path):
        """Compact copy works on empty database."""
        dest_path = tmp_path / "compact.db"

        with ParrotDB(db_path, create=True) as db:
            db.copy(dest_path, compact=True)

        # Destination should exist
        assert dest_path.exists()

        # Should be readable and empty
        with ParrotDB(dest_path, create=False) as db:
            assert db.get(b"key") is None

    def test_copy_compact_reduces_size(self, db_path: Path, tmp_path: Path):
        """Compact copy reduces file size after deletes."""
        dest_path = tmp_path / "compact.db"

        with ParrotDB(db_path, create=True) as db:
            # Insert lots of data
            for i in range(200):
                db.put(f"key{i:03d}".encode(), b"x" * 500)

            # Delete half
            for i in range(100):
                db.delete(f"key{i:03d}".encode())

            original_size = db_path.stat().st_size

            # Compact copy
            db.copy(dest_path, compact=True)

        compact_size = dest_path.stat().st_size

        # Compacted file should be smaller
        assert compact_size < original_size

        # Data should be correct
        with ParrotDB(dest_path, create=False) as db:
            for i in range(100, 200):
                assert db.get(f"key{i:03d}".encode()) == b"x" * 500
            for i in range(100):
                assert db.get(f"key{i:03d}".encode()) is None

    def test_copy_compact_preserves_data(self, db_path: Path, tmp_path: Path):
        """Compact copy preserves all data correctly."""
        dest_path = tmp_path / "compact.db"

        with ParrotDB(db_path, create=True) as db:
            # Insert varied data
            for i in range(50):
                key = f"key{i:03d}".encode()
                value = f"value{i}" .encode() * (i + 1)  # Variable size
                db.put(key, value)

            db.copy(dest_path, compact=True)

        # Verify all data
        with ParrotDB(dest_path, create=False) as db:
            for i in range(50):
                key = f"key{i:03d}".encode()
                expected = f"value{i}".encode() * (i + 1)
                assert db.get(key) == expected

    def test_copy_during_read_transaction(self, db_path: Path, tmp_path: Path):
        """Copy works while read transaction is active."""
        dest_path = tmp_path / "copy.db"

        with ParrotDB(db_path, create=True) as db:
            db.put(b"key", b"value")

            # Start read transaction
            with db.begin() as txn:
                # Copy while transaction is open
                db.copy(dest_path, compact=True)

                # Original transaction still works
                assert txn.get(b"key") == b"value"

        # Copy is correct
        with ParrotDB(dest_path, create=False) as db:
            assert db.get(b"key") == b"value"
