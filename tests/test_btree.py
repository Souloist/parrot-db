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


class TestBTreeByteSizeSplit:
    """Test that leaf splits use byte size, not cell count."""

    def test_skewed_cell_sizes_causes_overflow_with_count_split(self, pager: Pager):
        """Demonstrates bug: count-based split overflows when large cell is in right half.

        Setup: Fill a leaf with ~104 small cells (max that fits), then insert one
        large cell that sorts last. Count-based split puts 52 small cells on left
        and 52 small + 1 large on right. Right half overflows.

        Page math (4096 byte page):
        - Fixed overhead: 15 bytes (9 header + 6 leaf header)
        - Per cell: 2 (offset) + 4 (cell header) + key_len + value_len
        - Small cell (4-byte key, 33-byte value): 2 + 4 + 4 + 33 = 43 bytes
        - Large cell (4-byte key, 3900-byte value): 2 + 4 + 4 + 3900 = 3910 bytes
        - 104 small cells: 15 + 104*43 = 4487 bytes (too big, but close to split point)

        With count-based split at mid=52:
        - Right half: 52 small + 1 large = 15 + 53*2 + 52*41 + 3908 = 6159 > 4096!
        """
        btree = BTree(pager)
        root = 0

        # Insert small cells that sort before the large one
        # Each: 4-byte key + 33-byte value = 37 data bytes + 6 overhead = 43 total
        for i in range(100):
            key = f"a{i:03d}".encode()  # 4 bytes: a000, a001, etc.
            value = b"s" * 33  # 33 bytes
            root = btree.insert(root, key, value)

        # Insert large cell that sorts LAST - this triggers the problematic split
        # With count-based split, this ends up in right half with ~50 small cells
        large_key = b"zzzz"  # Sorts after all a### keys
        large_value = b"L" * 3900
        root = btree.insert(root, large_key, large_value)

        # If bug exists, this raises "Page overflow" during split
        # Verify all keys retrievable
        assert btree.get(root, large_key) == large_value
        for i in range(100):
            key = f"a{i:03d}".encode()
            assert btree.get(root, key) == b"s" * 33

    def test_large_cell_at_start_with_many_small(self, pager: Pager):
        """Large cell sorting first with many small cells following."""
        btree = BTree(pager)
        root = 0

        # Large cell sorts first
        large_key = b"aaaa"
        large_value = b"L" * 3900
        root = btree.insert(root, large_key, large_value)

        # Add small cells that sort after
        for i in range(100):
            key = f"b{i:03d}".encode()
            value = b"s" * 33
            root = btree.insert(root, key, value)

        # Verify all keys
        assert btree.get(root, large_key) == large_value
        for i in range(100):
            key = f"b{i:03d}".encode()
            assert btree.get(root, key) == b"s" * 33

    def test_multiple_large_values_split_correctly(self, pager: Pager):
        """Multiple large values should be distributed across splits properly."""
        btree = BTree(pager)
        root = 0

        # Insert several medium-large values
        for i in range(10):
            key = f"key{i:02d}".encode()
            value = b"v" * 800  # Each ~800 bytes, 10 would be ~8KB (needs split)
            root = btree.insert(root, key, value)

        # Verify all retrievable
        for i in range(10):
            key = f"key{i:02d}".encode()
            assert btree.get(root, key) == b"v" * 800

    def test_branch_split_with_skewed_separator_sizes(self, pager: Pager):
        """Direct test: branch split with skewed separator sizes should not overflow.

        Creates a pathological case by directly calling _split_branch with
        many small keys followed by a few large keys. Count-based split (mid=len//2)
        would put all large keys on right side, causing overflow.

        Branch page math (4096 bytes):
        - Fixed overhead: ~19 bytes (header + first child pointer)
        - Per separator: 2 (key_len) + key_size + 4 (child pointer)
        - Small key (5 bytes): 11 bytes per separator
        - Large key (500 bytes): 506 bytes per separator

        With 200 small + 10 large keys (210 total):
        - Count-based mid=105: Right gets 95 small + 10 large = 95*11 + 10*506 = 6105 bytes OVERFLOW
        """
        btree = BTree(pager)

        # Create small separator keys
        small_keys = [f"a{i:04d}".encode() for i in range(200)]  # 5 bytes each
        # Create large separator keys that sort after small ones
        large_keys = [f"z{i:04d}".encode() + b"x" * 495 for i in range(10)]  # 500 bytes each

        all_keys = small_keys + large_keys  # 210 keys, sorted order
        children = list(range(1000, 1000 + len(all_keys) + 1))  # Dummy child page IDs

        # This would fail with count-based split due to overflow
        result = btree._split_branch(all_keys, children)

        # Verify split succeeded and returned valid result
        assert result.split is not None
        assert result.split.left_page_id != 0
        assert result.split.right_page_id != 0

        # Read back both branches and verify they fit (didn't overflow)
        left_branch = pager.read_branch_page(result.split.left_page_id)
        right_branch = pager.read_branch_page(result.split.right_page_id)

        # Both should have been written successfully (no overflow error)
        assert len(left_branch.keys) > 0
        assert len(right_branch.keys) > 0

    def test_branch_split_natural_workload(self, pager: Pager):
        """Integration test: variable-length keys in realistic workload."""
        btree = BTree(pager)
        root = 0

        # Insert entries that create variable-length separators
        for i in range(2000):
            # Alternate between short and long keys
            if i % 10 == 0:
                key = f"long{i:04d}".encode() + b"x" * 90  # ~100 bytes
            else:
                key = f"k{i:04d}".encode()  # ~6 bytes
            value = b"v" * 20
            root = btree.insert(root, key, value)

        # Verify tree is valid
        assert btree.tree_height(root) >= 2
        count = btree.count_keys(root)
        assert count == 2000
