"""Tests for the copy-on-write B+ tree implementation."""

from pathlib import Path

import pytest

from storage import BTree, Pager


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def pager(db_path: Path) -> Pager:
    """Create a fresh pager for testing."""
    with Pager(db_path, create=True) as p:
        yield p


class TestBTreeBasicOperations:
    """Test basic get/insert/delete operations."""

    def test_empty_tree_get_returns_none(self, pager: Pager):
        btree = BTree(pager)
        assert btree.get(0, b"key") is None

    def test_insert_and_get_single_key(self, pager: Pager):
        btree = BTree(pager)
        root = btree.insert(0, b"key", b"value")
        assert root != 0
        assert btree.get(root, b"key") == b"value"

    def test_insert_and_get_multiple_keys(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        # Insert multiple keys
        for i in range(10):
            key = f"key{i:03d}".encode()
            value = f"value{i}".encode()
            root = btree.insert(root, key, value)

        # Verify all keys
        for i in range(10):
            key = f"key{i:03d}".encode()
            expected = f"value{i}".encode()
            assert btree.get(root, key) == expected

    def test_insert_overwrites_existing_key(self, pager: Pager):
        btree = BTree(pager)
        root = btree.insert(0, b"key", b"value1")
        root = btree.insert(root, b"key", b"value2")
        assert btree.get(root, b"key") == b"value2"

    def test_get_nonexistent_key_returns_none(self, pager: Pager):
        btree = BTree(pager)
        root = btree.insert(0, b"key", b"value")
        assert btree.get(root, b"other") is None

    def test_delete_from_empty_tree(self, pager: Pager):
        btree = BTree(pager)
        root = btree.delete(0, b"key")
        assert root == 0

    def test_delete_existing_key(self, pager: Pager):
        btree = BTree(pager)
        root = btree.insert(0, b"key", b"value")
        root = btree.delete(root, b"key")
        assert btree.get(root, b"key") is None

    def test_delete_nonexistent_key_returns_same_root(self, pager: Pager):
        btree = BTree(pager)
        root = btree.insert(0, b"key", b"value")
        new_root = btree.delete(root, b"other")
        # When key not found, we return the original root
        assert btree.get(new_root, b"key") == b"value"

    def test_delete_all_keys_returns_empty(self, pager: Pager):
        btree = BTree(pager)
        root = btree.insert(0, b"key1", b"value1")
        root = btree.insert(root, b"key2", b"value2")
        root = btree.delete(root, b"key1")
        root = btree.delete(root, b"key2")
        assert root == 0


class TestBTreeCopyOnWrite:
    """Test copy-on-write semantics."""

    def test_insert_returns_new_root(self, pager: Pager):
        btree = BTree(pager)
        root1 = btree.insert(0, b"key1", b"value1")
        root2 = btree.insert(root1, b"key2", b"value2")
        assert root1 != root2

    def test_old_root_still_valid_after_insert(self, pager: Pager):
        btree = BTree(pager)
        root1 = btree.insert(0, b"key1", b"value1")

        # Hold reference to old root
        old_root = root1

        # Make mutations
        root2 = btree.insert(root1, b"key2", b"value2")
        root3 = btree.insert(root2, b"key3", b"value3")

        # Old root still returns old data
        assert btree.get(old_root, b"key1") == b"value1"
        assert btree.get(old_root, b"key2") is None
        assert btree.get(old_root, b"key3") is None

        # New root has all data
        assert btree.get(root3, b"key1") == b"value1"
        assert btree.get(root3, b"key2") == b"value2"
        assert btree.get(root3, b"key3") == b"value3"

    def test_old_root_valid_after_delete(self, pager: Pager):
        btree = BTree(pager)
        root1 = btree.insert(0, b"key1", b"value1")
        root2 = btree.insert(root1, b"key2", b"value2")

        # Hold reference
        old_root = root2

        # Delete from new tree
        root3 = btree.delete(root2, b"key1")

        # Old root still has both keys
        assert btree.get(old_root, b"key1") == b"value1"
        assert btree.get(old_root, b"key2") == b"value2"

        # New root only has key2
        assert btree.get(root3, b"key1") is None
        assert btree.get(root3, b"key2") == b"value2"

    def test_update_preserves_old_root(self, pager: Pager):
        btree = BTree(pager)
        root1 = btree.insert(0, b"key", b"old_value")
        root2 = btree.insert(root1, b"key", b"new_value")

        assert btree.get(root1, b"key") == b"old_value"
        assert btree.get(root2, b"key") == b"new_value"


class TestBTreeRangeScan:
    """Test range scan operations."""

    def test_range_scan_empty_tree(self, pager: Pager):
        btree = BTree(pager)
        results = list(btree.range_scan(0))
        assert results == []

    def test_range_scan_all_keys(self, pager: Pager):
        btree = BTree(pager)
        root = 0
        expected = []

        for i in range(10):
            key = f"key{i:03d}".encode()
            value = f"value{i}".encode()
            root = btree.insert(root, key, value)
            expected.append((key, value))

        expected.sort()
        results = list(btree.range_scan(root))
        assert results == expected

    def test_range_scan_with_start_key(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        for i in range(10):
            key = f"key{i:03d}".encode()
            value = f"value{i}".encode()
            root = btree.insert(root, key, value)

        # Start from key005
        results = list(btree.range_scan(root, start=b"key005"))
        keys = [k for k, _ in results]
        assert keys == [b"key005", b"key006", b"key007", b"key008", b"key009"]

    def test_range_scan_with_end_key(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        for i in range(10):
            key = f"key{i:03d}".encode()
            value = f"value{i}".encode()
            root = btree.insert(root, key, value)

        # End before key005 (exclusive)
        results = list(btree.range_scan(root, end=b"key005"))
        keys = [k for k, _ in results]
        assert keys == [b"key000", b"key001", b"key002", b"key003", b"key004"]

    def test_range_scan_with_start_and_end(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        for i in range(10):
            key = f"key{i:03d}".encode()
            value = f"value{i}".encode()
            root = btree.insert(root, key, value)

        results = list(btree.range_scan(root, start=b"key003", end=b"key007"))
        keys = [k for k, _ in results]
        assert keys == [b"key003", b"key004", b"key005", b"key006"]

    def test_range_scan_returns_sorted_order(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        # Insert in random order
        for key in [b"zebra", b"apple", b"mango", b"banana"]:
            root = btree.insert(root, key, b"value")

        results = list(btree.range_scan(root))
        keys = [k for k, _ in results]
        assert keys == [b"apple", b"banana", b"mango", b"zebra"]

    def test_range_scan_start_equals_separator_key(self, pager: Pager):
        """Test range scan when start key equals a separator in a branch node.

        This tests the pruning logic - when start equals a separator key,
        the scan must still include that key (which is in the right subtree).
        """
        btree = BTree(pager)
        root = 0

        # Insert enough keys to force splits and create branch nodes
        # With 4KB pages, ~500 small keys will create a multi-level tree
        all_keys = [f"key{i:05d}".encode() for i in range(500)]
        for key in all_keys:
            root = btree.insert(root, key, b"value")

        # Verify tree has height > 1 (has branch nodes)
        height = btree.tree_height(root)
        assert height > 1, "Test requires multi-level tree"

        # Collect ALL separator keys from ALL branch nodes in the tree
        from storage.pages import BranchPage

        def collect_separators(page_id: int) -> list[bytes]:
            """Recursively collect all separator keys from branch nodes."""
            page_data = pager.read_page_raw(page_id)
            page_type = page_data[0]
            if page_type != 3:  # Not a branch
                return []
            branch = BranchPage.from_bytes(page_data)
            separators = list(branch.keys)
            for child_id in branch.children:
                separators.extend(collect_separators(child_id))
            return separators

        all_separators = collect_separators(root)
        assert len(all_separators) > 0, "Should have separator keys"

        # Test EVERY separator key
        for separator in all_separators:
            results = list(btree.range_scan(root, start=separator))
            result_keys = [k for k, _ in results]

            # The separator key must be included (it exists in the tree)
            assert separator in result_keys, f"Separator key {separator!r} missing from range scan results"
            # It should be the first result when starting from it
            assert result_keys[0] == separator, f"First result should be {separator!r}, got {result_keys[0]!r}"


class TestBTreeSplitting:
    """Test node splitting behavior."""

    def test_leaf_split_preserves_data(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        # Insert enough keys to force a split
        # With 4KB pages, we can fit roughly 100+ small key-value pairs per leaf
        for i in range(200):
            key = f"key{i:05d}".encode()
            value = f"value{i:05d}".encode()
            root = btree.insert(root, key, value)

        # Verify all keys are retrievable
        for i in range(200):
            key = f"key{i:05d}".encode()
            expected = f"value{i:05d}".encode()
            assert btree.get(root, key) == expected, f"Failed for {key}"

    def test_large_tree_maintains_correctness(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        # Insert 10,000 keys (acceptance criteria)
        for i in range(10000):
            key = f"key{i:08d}".encode()
            value = f"value{i:08d}".encode()
            root = btree.insert(root, key, value)

        # Verify all keys
        for i in range(10000):
            key = f"key{i:08d}".encode()
            expected = f"value{i:08d}".encode()
            assert btree.get(root, key) == expected, f"Failed for {key}"

    def test_tree_height_bounded(self, pager: Pager):
        """Tree height should be <= 4 for 10k keys (acceptance criteria)."""
        btree = BTree(pager)
        root = 0

        for i in range(10000):
            key = f"key{i:08d}".encode()
            value = f"value{i:08d}".encode()
            root = btree.insert(root, key, value)

        height = btree.tree_height(root)
        assert height <= 4, f"Tree height {height} exceeds limit of 4"

    def test_branch_split_preserves_structure(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        # Insert enough to cause branch splits (need many leaf splits first)
        for i in range(5000):
            key = f"key{i:06d}".encode()
            value = f"value{i:06d}".encode()
            root = btree.insert(root, key, value)

        # Range scan should return all keys in sorted order
        results = list(btree.range_scan(root))
        assert len(results) == 5000

        keys = [k for k, _ in results]
        assert keys == sorted(keys)


class TestBTreeEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_key(self, pager: Pager):
        btree = BTree(pager)
        root = btree.insert(0, b"", b"empty_key_value")
        assert btree.get(root, b"") == b"empty_key_value"

    def test_empty_value(self, pager: Pager):
        btree = BTree(pager)
        root = btree.insert(0, b"key", b"")
        assert btree.get(root, b"key") == b""

    def test_binary_keys(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        # Insert keys with binary data
        for i in range(256):
            key = bytes([i])
            value = f"value_{i}".encode()
            root = btree.insert(root, key, value)

        # Verify all retrieved
        for i in range(256):
            key = bytes([i])
            expected = f"value_{i}".encode()
            assert btree.get(root, key) == expected

    def test_large_key_value(self, pager: Pager):
        btree = BTree(pager)
        # Single large key-value that fits in a page
        large_key = b"k" * 100
        large_value = b"v" * 1000
        root = btree.insert(0, large_key, large_value)
        assert btree.get(root, large_key) == large_value

    def test_delete_middle_key_preserves_others(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        # Insert keys
        for i in range(5):
            root = btree.insert(root, f"key{i}".encode(), f"value{i}".encode())

        # Delete middle key
        root = btree.delete(root, b"key2")

        # Others should remain
        assert btree.get(root, b"key0") == b"value0"
        assert btree.get(root, b"key1") == b"value1"
        assert btree.get(root, b"key2") is None
        assert btree.get(root, b"key3") == b"value3"
        assert btree.get(root, b"key4") == b"value4"

    def test_delete_first_key(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        for i in range(5):
            root = btree.insert(root, f"key{i}".encode(), f"value{i}".encode())

        root = btree.delete(root, b"key0")
        assert btree.get(root, b"key0") is None
        assert btree.get(root, b"key1") == b"value1"

    def test_delete_last_key(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        for i in range(5):
            root = btree.insert(root, f"key{i}".encode(), f"value{i}".encode())

        root = btree.delete(root, b"key4")
        assert btree.get(root, b"key4") is None
        assert btree.get(root, b"key3") == b"value3"


class TestBTreeCount:
    """Test key counting functionality."""

    def test_count_empty_tree(self, pager: Pager):
        btree = BTree(pager)
        assert btree.count_keys(0) == 0

    def test_count_after_inserts(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        for i in range(100):
            root = btree.insert(root, f"key{i}".encode(), b"value")

        assert btree.count_keys(root) == 100

    def test_count_after_deletes(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        for i in range(100):
            root = btree.insert(root, f"key{i:03d}".encode(), b"value")

        for i in range(50):
            root = btree.delete(root, f"key{i:03d}".encode())

        assert btree.count_keys(root) == 50


class TestBTreeHeight:
    """Test tree height calculations."""

    def test_height_empty_tree(self, pager: Pager):
        btree = BTree(pager)
        assert btree.tree_height(0) == 0

    def test_height_single_leaf(self, pager: Pager):
        btree = BTree(pager)
        root = btree.insert(0, b"key", b"value")
        assert btree.tree_height(root) == 1

    def test_height_increases_with_splits(self, pager: Pager):
        btree = BTree(pager)
        root = 0

        # Insert until we get height > 1
        heights_seen = {0}
        for i in range(1000):
            root = btree.insert(root, f"key{i:06d}".encode(), b"value")
            h = btree.tree_height(root)
            heights_seen.add(h)

        # Should have seen multiple heights as tree grew
        assert len(heights_seen) > 1


class TestBTreePersistence:
    """Test that tree persists correctly across pager sessions."""

    def test_tree_persists_across_sessions(self, db_path: Path):
        # Create tree in first session
        with Pager(db_path, create=True) as pager:
            btree = BTree(pager)
            root = 0
            for i in range(100):
                root = btree.insert(root, f"key{i:03d}".encode(), f"value{i}".encode())
            stored_root = root

        # Read tree in second session
        with Pager(db_path, create=False) as pager:
            btree = BTree(pager)
            for i in range(100):
                key = f"key{i:03d}".encode()
                expected = f"value{i}".encode()
                assert btree.get(stored_root, key) == expected
