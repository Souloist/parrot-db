"""Tests for page-based storage (Stage 2)."""

import tempfile
from pathlib import Path

import pytest

from storage.freelist import Freelist
from storage.pager import (
    FIRST_DATA_PAGE_ID,
    HEADER_PAGE_ID,
    META_PAGE_0_ID,
    META_PAGE_1_ID,
    Pager,
)
from storage.pages import (
    BranchPage,
    FreelistPage,
    HeaderPage,
    LeafPage,
    MetaPage,
)


class TestHeaderPage:
    """Tests for HeaderPage encoding/decoding."""

    def test_round_trip(self):
        header = HeaderPage(page_size=4096)
        data = header.to_bytes(4096)
        restored = HeaderPage.from_bytes(data)

        assert restored.magic == header.magic
        assert restored.version == header.version
        assert restored.page_size == header.page_size

    def test_custom_page_size(self):
        header = HeaderPage(page_size=8192)
        data = header.to_bytes(8192)
        restored = HeaderPage.from_bytes(data)

        assert restored.page_size == 8192

    def test_invalid_magic(self):
        data = b"XXXX" + b"\x00" * 100
        with pytest.raises(ValueError, match="Invalid magic bytes"):
            HeaderPage.from_bytes(data)


class TestMetaPage:
    """Tests for MetaPage encoding/decoding."""

    def test_round_trip(self):
        meta = MetaPage(page_id=1, txn_id=42, root_page_id=10, freelist_page_id=5)
        data = meta.to_bytes(4096)
        restored = MetaPage.from_bytes(data)

        assert restored.page_id == meta.page_id
        assert restored.txn_id == meta.txn_id
        assert restored.root_page_id == meta.root_page_id
        assert restored.freelist_page_id == meta.freelist_page_id

    def test_checksum_validation(self):
        meta = MetaPage(page_id=1, txn_id=42)
        data = bytearray(meta.to_bytes(4096))
        # Corrupt the data
        data[20] ^= 0xFF
        with pytest.raises(ValueError, match="Checksum mismatch"):
            MetaPage.from_bytes(bytes(data))

    def test_checksum_skip(self):
        meta = MetaPage(page_id=1, txn_id=42)
        data = bytearray(meta.to_bytes(4096))
        # Corrupt the data but skip checksum verification
        data[20] ^= 0xFF
        restored = MetaPage.from_bytes(bytes(data), verify_checksum=False)
        assert restored.page_id == 1


class TestFreelistPage:
    """Tests for FreelistPage encoding/decoding."""

    def test_round_trip_empty(self):
        page = FreelistPage(page_id=3, free_page_ids=[])
        data = page.to_bytes(4096)
        restored = FreelistPage.from_bytes(data)

        assert restored.page_id == page.page_id
        assert restored.free_page_ids == []

    def test_round_trip_with_pages(self):
        page = FreelistPage(page_id=3, free_page_ids=[10, 20, 30, 40])
        data = page.to_bytes(4096)
        restored = FreelistPage.from_bytes(data)

        assert restored.free_page_ids == [10, 20, 30, 40]

    def test_round_trip_many_pages(self):
        page_ids = list(range(100, 500))
        page = FreelistPage(page_id=3, free_page_ids=page_ids)
        data = page.to_bytes(4096)
        restored = FreelistPage.from_bytes(data)

        assert restored.free_page_ids == page_ids

    def test_checksum_validation(self):
        page = FreelistPage(page_id=3, free_page_ids=[10, 20])
        data = bytearray(page.to_bytes(4096))
        data[15] ^= 0xFF  # Corrupt data
        with pytest.raises(ValueError, match="Checksum mismatch"):
            FreelistPage.from_bytes(bytes(data))


class TestLeafPage:
    """Tests for LeafPage encoding/decoding."""

    def test_round_trip_empty(self):
        page = LeafPage(page_id=5, cells=[])
        data = page.to_bytes(4096)
        restored = LeafPage.from_bytes(data)

        assert restored.page_id == page.page_id
        assert restored.cells == []
        assert restored.right_sibling == 0

    def test_round_trip_with_cells(self):
        cells = [(b"key1", b"value1"), (b"key2", b"value2")]
        page = LeafPage(page_id=5, cells=cells, right_sibling=6)
        data = page.to_bytes(4096)
        restored = LeafPage.from_bytes(data)

        assert restored.cells == cells
        assert restored.right_sibling == 6

    def test_round_trip_binary_data(self):
        cells = [(b"\x00\x01\x02", b"\xff\xfe\xfd"), (b"key", b"\x00" * 100)]
        page = LeafPage(page_id=5, cells=cells)
        data = page.to_bytes(4096)
        restored = LeafPage.from_bytes(data)

        assert restored.cells == cells

    def test_checksum_validation(self):
        page = LeafPage(page_id=5, cells=[(b"key", b"value")])
        data = bytearray(page.to_bytes(4096))
        data[100] ^= 0xFF  # Corrupt data
        with pytest.raises(ValueError, match="Checksum mismatch"):
            LeafPage.from_bytes(bytes(data))


class TestBranchPage:
    """Tests for BranchPage encoding/decoding."""

    def test_round_trip_single_child(self):
        page = BranchPage(page_id=4, keys=[], children=[10])
        data = page.to_bytes(4096)
        restored = BranchPage.from_bytes(data)

        assert restored.page_id == page.page_id
        assert restored.keys == []
        assert restored.children == [10]

    def test_round_trip_with_keys(self):
        page = BranchPage(page_id=4, keys=[b"mid"], children=[10, 20])
        data = page.to_bytes(4096)
        restored = BranchPage.from_bytes(data)

        assert restored.keys == [b"mid"]
        assert restored.children == [10, 20]

    def test_round_trip_multiple_keys(self):
        keys = [b"aaa", b"bbb", b"ccc"]
        children = [10, 20, 30, 40]
        page = BranchPage(page_id=4, keys=keys, children=children)
        data = page.to_bytes(4096)
        restored = BranchPage.from_bytes(data)

        assert restored.keys == keys
        assert restored.children == children

    def test_checksum_validation(self):
        page = BranchPage(page_id=4, keys=[b"key"], children=[10, 20])
        data = bytearray(page.to_bytes(4096))
        data[20] ^= 0xFF  # Corrupt data
        with pytest.raises(ValueError, match="Checksum mismatch"):
            BranchPage.from_bytes(bytes(data))


class TestFreelist:
    """Tests for in-memory Freelist."""

    def test_allocate_from_empty(self):
        fl = Freelist()
        assert fl.allocate() is None
        assert fl.is_empty()

    def test_free_and_allocate(self):
        fl = Freelist()
        fl.free(10)
        fl.free(20)

        assert fl.count() == 2
        assert not fl.is_empty()

        allocated = fl.allocate()
        assert allocated in (10, 20)
        assert fl.count() == 1

    def test_contains(self):
        fl = Freelist()
        fl.free(10)

        assert fl.contains(10)
        assert not fl.contains(20)

    def test_free_many(self):
        fl = Freelist()
        fl.free_many([10, 20, 30])

        assert fl.count() == 3
        assert fl.contains(10)
        assert fl.contains(20)
        assert fl.contains(30)

    def test_to_list_sorted(self):
        fl = Freelist()
        fl.free_many([30, 10, 20])

        assert fl.to_list() == [10, 20, 30]

    def test_to_page_and_back(self):
        fl = Freelist()
        fl.free_many([10, 20, 30])

        page = fl.to_page(page_id=5)
        restored = Freelist.from_page(page)

        assert restored.to_list() == fl.to_list()

    def test_clear(self):
        fl = Freelist()
        fl.free_many([10, 20])
        fl.clear()

        assert fl.is_empty()
        assert fl.count() == 0


class TestPager:
    """Tests for Pager file I/O."""

    def test_create_new_database(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with Pager(db_path) as pager:
                # Verify header
                header = pager.read_header()
                assert header.magic == b"PRRT"
                assert header.page_size == 4096

                # Verify meta pages exist
                meta0 = pager.read_meta_page(META_PAGE_0_ID)
                meta1 = pager.read_meta_page(META_PAGE_1_ID)
                assert meta0.txn_id == 0
                assert meta1.txn_id == 0

                # Verify page count
                assert pager.page_count == FIRST_DATA_PAGE_ID

    def test_open_existing_database(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # Create database
            with Pager(db_path, page_size=8192) as pager:
                pass

            # Reopen
            with Pager(db_path) as pager:
                header = pager.read_header()
                assert header.page_size == 8192

    def test_write_and_read_leaf_page(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with Pager(db_path) as pager:
                page_id = pager.allocate_page()
                leaf = LeafPage(page_id=page_id, cells=[(b"key", b"value")])
                pager.write_leaf_page(leaf)

                restored = pager.read_leaf_page(page_id)
                assert restored.cells == [(b"key", b"value")]

    def test_write_and_read_branch_page(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with Pager(db_path) as pager:
                page_id = pager.allocate_page()
                branch = BranchPage(page_id=page_id, keys=[b"mid"], children=[10, 20])
                pager.write_branch_page(branch)

                restored = pager.read_branch_page(page_id)
                assert restored.keys == [b"mid"]
                assert restored.children == [10, 20]

    def test_allocate_reuses_freed_pages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with Pager(db_path) as pager:
                # Allocate some pages
                p1 = pager.allocate_page()
                _p2 = pager.allocate_page()  # noqa: F841

                # Free one
                pager.free_page(p1)

                # Next allocation should reuse freed page
                p3 = pager.allocate_page()
                assert p3 == p1

    def test_allocate_extends_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with Pager(db_path) as pager:
                initial_count = pager.page_count

                p1 = pager.allocate_page()
                assert p1 == initial_count

                p2 = pager.allocate_page()
                assert p2 == initial_count + 1

    def test_meta_page_update(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with Pager(db_path) as pager:
                # Write to inactive meta page
                inactive_id = pager.get_inactive_meta_id()
                meta = MetaPage(page_id=inactive_id, txn_id=1, root_page_id=10)
                pager.write_meta_page(meta)
                pager.sync()

                # Active should now be the one we just wrote
                active = pager.read_active_meta()
                assert active.txn_id == 1
                assert active.root_page_id == 10

    def test_corrupt_page_detected(self):
        """Acceptance: corrupt a page -> checksum detects it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with Pager(db_path) as pager:
                page_id = pager.allocate_page()
                leaf = LeafPage(page_id=page_id, cells=[(b"key", b"value")])
                pager.write_leaf_page(leaf)
                pager.sync()

            # Corrupt the page directly in the file
            with open(db_path, "r+b") as f:
                f.seek(page_id * 4096 + 100)  # Somewhere in the page
                f.write(b"\xff\xff\xff\xff")

            # Reopen and try to read
            with Pager(db_path) as pager:
                with pytest.raises(ValueError, match="Checksum mismatch"):
                    pager.read_leaf_page(page_id)

    def test_cannot_free_reserved_pages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with Pager(db_path) as pager:
                with pytest.raises(ValueError, match="Cannot free reserved page"):
                    pager.free_page(HEADER_PAGE_ID)
                with pytest.raises(ValueError, match="Cannot free reserved page"):
                    pager.free_page(META_PAGE_0_ID)

    def test_file_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nonexistent.db"

            with pytest.raises(FileNotFoundError):
                Pager(db_path, create=False)


class TestInspectTool:
    """Tests to verify db_inspect.py works correctly."""

    def test_inspect_summary(self):
        """Acceptance: tools/db_inspect.py --summary shows file structure."""
        import subprocess

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # Create a database
            with Pager(db_path) as pager:
                page_id = pager.allocate_page()
                leaf = LeafPage(page_id=page_id, cells=[(b"test", b"data")])
                pager.write_leaf_page(leaf)
                pager.sync()

            # Run inspect tool
            result = subprocess.run(
                ["uv", "run", "python", "tools/db_inspect.py", "--db", str(db_path), "--summary"],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent,
            )

            assert result.returncode == 0
            assert "DATABASE SUMMARY" in result.stdout
            assert "Header Page" in result.stdout
            assert "Meta Pages" in result.stdout
            assert "PRRT" in result.stdout
