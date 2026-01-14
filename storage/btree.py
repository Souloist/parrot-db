"""Copy-on-write B+ tree

The B+ tree stores key-value pairs in leaf nodes, with internal branch nodes
containing separator keys to guide searches. All mutations create new pages
instead of modifying existing ones (copy-on-write), enabling:

- Atomic visibility: new tree visible only when root pointer is updated
- Snapshot isolation: old root remains valid for concurrent readers
- Crash safety: incomplete writes leave old tree intact

Key operations return a new root_page_id on mutation. The caller is responsible
for updating the root pointer atomically (via meta page swap).
"""

import bisect
from collections.abc import Iterator
from dataclasses import dataclass

from storage.pager import Pager
from storage.pages import BranchPage, LeafPage

# Minimum fill factor before considering merge (not implemented in this stage)
MIN_KEYS_PER_LEAF = 1
MIN_KEYS_PER_BRANCH = 1


@dataclass
class SplitResult:
    """Result of splitting a node during insert."""

    left_page_id: int
    right_page_id: int
    separator_key: bytes  # First key in the right node


@dataclass
class InsertResult:
    """Result of an insert operation at any level."""

    new_page_id: int
    split: SplitResult | None = None


@dataclass
class DeleteResult:
    """Result of a delete operation."""

    new_page_id: int  # 0 if node became empty
    deleted: bool = False


class BTree:
    """Copy-on-write B+ tree with path copying.

    All mutations return a new root page_id. The original tree remains valid
    and accessible via its old root, enabling snapshot isolation.
    """

    def __init__(self, pager: Pager, min_keys_per_leaf: int = MIN_KEYS_PER_LEAF):
        self.pager = pager
        self.min_keys_per_leaf = min_keys_per_leaf
        self._page_size = pager.page_size

        # Page header (9) + leaf header cell_count (2) + right_sibling (4)
        self._leaf_header_size = 9 + 2 + 4
        # Page header (9) + key_count (2) + first child (4)
        self._branch_header_size = 9 + 2 + 4

    def _leaf_fits(self, cells: list[tuple[bytes, bytes]]) -> bool:
        """Check if cells fit in a single leaf page."""
        # Fixed overhead + cell offsets (2 per cell) + cell data
        # Cell data: key_len (2) + value_len (2) + key + value
        overhead = self._leaf_header_size + 2 * len(cells)
        data_size = sum(4 + len(k) + len(v) for k, v in cells)
        return overhead + data_size <= self._page_size

    def _branch_fits(self, keys: list[bytes], children: list[int]) -> bool:
        """Check if keys and children fit in a single branch page."""
        # Fixed overhead + key data (2 + len for each) + child pointers (4 for rest)
        overhead = self._branch_header_size
        keys_size = sum(2 + len(k) + 4 for k in keys)  # key_len + key + child
        return overhead + keys_size <= self._page_size

    def get(self, root_page_id: int, key: bytes) -> bytes | None:
        """Look up a key starting from the given root.

        Returns the value if found, None otherwise.
        """
        if root_page_id == 0:
            return None

        return self._search(root_page_id, key)

    def _search(self, page_id: int, key: bytes) -> bytes | None:
        """Recursively search for a key starting from page_id."""
        page_data = self.pager._read_page_raw(page_id)

        # Check page type from first byte
        page_type = page_data[0]

        if page_type == 4:  # LEAF
            leaf = LeafPage.from_bytes(page_data)
            return self._search_leaf(leaf, key)
        elif page_type == 3:  # BRANCH
            branch = BranchPage.from_bytes(page_data)
            child_page_id = self._find_child(branch, key)
            return self._search(child_page_id, key)
        else:
            raise ValueError(f"Unexpected page type: {page_type}")

    def _search_leaf(self, leaf: LeafPage, key: bytes) -> bytes | None:
        """Binary search for key in leaf cells."""
        keys = [k for k, _ in leaf.cells]
        idx = bisect.bisect_left(keys, key)
        if idx < len(leaf.cells) and leaf.cells[idx][0] == key:
            return leaf.cells[idx][1]
        return None

    def _find_child(self, branch: BranchPage, key: bytes) -> int:
        """Find the child page_id for a key in a branch node.

        Uses binary search on separator keys. The key belongs to children[i]
        where i is the number of keys less than or equal to the search key.
        """
        idx = bisect.bisect_right(branch.keys, key)
        return branch.children[idx]

    def _find_leftmost_leaf(self, page_id: int) -> int:
        """Find the leftmost leaf page in the subtree rooted at page_id."""
        page_data = self.pager._read_page_raw(page_id)
        page_type = page_data[0]

        if page_type == 4:  # LEAF
            return page_id
        elif page_type == 3:  # BRANCH
            branch = BranchPage.from_bytes(page_data)
            return self._find_leftmost_leaf(branch.children[0])
        else:
            raise ValueError(f"Unexpected page type: {page_type}")

    def insert(self, root_page_id: int, key: bytes, value: bytes) -> int:
        """Insert a key-value pair, returning the new root page_id.

        If root_page_id is 0, creates a new tree with a single leaf.
        """
        if root_page_id == 0:
            # Create first leaf
            new_page_id = self.pager.allocate_page()
            leaf = LeafPage(page_id=new_page_id, cells=[(key, value)])
            self.pager.write_leaf_page(leaf)
            return new_page_id

        result = self._insert_recursive(root_page_id, key, value)

        if result.split is not None:
            # Root split - create new root branch
            new_root_id = self.pager.allocate_page()
            new_root = BranchPage(
                page_id=new_root_id,
                keys=[result.split.separator_key],
                children=[result.split.left_page_id, result.split.right_page_id],
            )
            self.pager.write_branch_page(new_root)
            return new_root_id

        return result.new_page_id

    def _insert_recursive(self, page_id: int, key: bytes, value: bytes) -> InsertResult:
        """Recursively insert into subtree rooted at page_id."""
        page_data = self.pager._read_page_raw(page_id)
        page_type = page_data[0]

        if page_type == 4:  # LEAF
            leaf = LeafPage.from_bytes(page_data)
            return self._insert_leaf(leaf, key, value)
        elif page_type == 3:  # BRANCH
            branch = BranchPage.from_bytes(page_data)
            return self._insert_branch(branch, key, value)
        else:
            raise ValueError(f"Unexpected page type: {page_type}")

    def _insert_leaf(self, leaf: LeafPage, key: bytes, value: bytes) -> InsertResult:
        """Insert into a leaf node, splitting if necessary."""
        # Find insertion point and check for existing key
        keys = [k for k, _ in leaf.cells]
        idx = bisect.bisect_left(keys, key)

        # Build new cells list
        new_cells = list(leaf.cells)
        if idx < len(new_cells) and new_cells[idx][0] == key:
            # Update existing key
            new_cells[idx] = (key, value)
        else:
            # Insert new key
            new_cells.insert(idx, (key, value))

        # Check if we need to split by calculating total size needed
        if self._leaf_fits(new_cells):
            # No split needed - create new leaf with all cells
            new_page_id = self.pager.allocate_page()
            new_leaf = LeafPage(page_id=new_page_id, cells=new_cells, right_sibling=leaf.right_sibling)
            self.pager.write_leaf_page(new_leaf)
            return InsertResult(new_page_id=new_page_id)
        else:
            # Need to split
            return self._split_leaf(new_cells, leaf.right_sibling)

    def _split_leaf(self, cells: list[tuple[bytes, bytes]], old_right_sibling: int) -> InsertResult:
        """Split a leaf into two leaves."""
        mid = len(cells) // 2

        left_cells = cells[:mid]
        right_cells = cells[mid:]

        # Allocate pages for both halves
        right_page_id = self.pager.allocate_page()
        left_page_id = self.pager.allocate_page()

        # Right leaf gets old sibling pointer
        right_leaf = LeafPage(page_id=right_page_id, cells=right_cells, right_sibling=old_right_sibling)

        # Left leaf points to right leaf
        left_leaf = LeafPage(page_id=left_page_id, cells=left_cells, right_sibling=right_page_id)

        self.pager.write_leaf_page(left_leaf)
        self.pager.write_leaf_page(right_leaf)

        # Separator is the first key in right leaf
        separator_key = right_cells[0][0]

        return InsertResult(
            new_page_id=left_page_id,
            split=SplitResult(
                left_page_id=left_page_id,
                right_page_id=right_page_id,
                separator_key=separator_key,
            ),
        )

    def _insert_branch(self, branch: BranchPage, key: bytes, value: bytes) -> InsertResult:
        """Insert into subtree via branch node."""
        # Find which child to insert into
        child_idx = bisect.bisect_right(branch.keys, key)
        child_page_id = branch.children[child_idx]

        # Recursively insert
        result = self._insert_recursive(child_page_id, key, value)

        if result.split is None:
            # No split below - just update child pointer
            new_children = list(branch.children)
            new_children[child_idx] = result.new_page_id

            new_page_id = self.pager.allocate_page()
            new_branch = BranchPage(page_id=new_page_id, keys=list(branch.keys), children=new_children)
            self.pager.write_branch_page(new_branch)
            return InsertResult(new_page_id=new_page_id)
        else:
            # Child split - need to insert new separator
            return self._insert_separator(
                branch,
                child_idx,
                result.split.separator_key,
                result.split.left_page_id,
                result.split.right_page_id,
            )

    def _insert_separator(
        self,
        branch: BranchPage,
        child_idx: int,
        separator_key: bytes,
        left_page_id: int,
        right_page_id: int,
    ) -> InsertResult:
        """Insert a separator key into a branch after a child split."""
        # Build new keys and children
        new_keys = list(branch.keys)
        new_children = list(branch.children)

        # Replace old child with left, insert right after it
        new_children[child_idx] = left_page_id
        new_keys.insert(child_idx, separator_key)
        new_children.insert(child_idx + 1, right_page_id)

        # Check if branch needs to split
        if self._branch_fits(new_keys, new_children):
            # No split needed
            new_page_id = self.pager.allocate_page()
            new_branch = BranchPage(page_id=new_page_id, keys=new_keys, children=new_children)
            self.pager.write_branch_page(new_branch)
            return InsertResult(new_page_id=new_page_id)
        else:
            # Need to split branch
            return self._split_branch(new_keys, new_children)

    def _split_branch(self, keys: list[bytes], children: list[int]) -> InsertResult:
        """Split a branch node into two branches."""
        mid = len(keys) // 2

        # Left gets keys[:mid] and children[:mid+1]
        left_keys = keys[:mid]
        left_children = children[: mid + 1]

        # Separator key is promoted to parent
        separator_key = keys[mid]

        # Right gets keys[mid+1:] and children[mid+1:]
        right_keys = keys[mid + 1 :]
        right_children = children[mid + 1 :]

        # Allocate pages
        left_page_id = self.pager.allocate_page()
        right_page_id = self.pager.allocate_page()

        left_branch = BranchPage(page_id=left_page_id, keys=left_keys, children=left_children)
        right_branch = BranchPage(page_id=right_page_id, keys=right_keys, children=right_children)

        self.pager.write_branch_page(left_branch)
        self.pager.write_branch_page(right_branch)

        return InsertResult(
            new_page_id=left_page_id,
            split=SplitResult(
                left_page_id=left_page_id,
                right_page_id=right_page_id,
                separator_key=separator_key,
            ),
        )

    def delete(self, root_page_id: int, key: bytes) -> int:
        """Delete a key, returning the new root page_id.

        Returns 0 if the tree becomes empty. Returns the same root if key not found.
        """
        if root_page_id == 0:
            return 0

        result = self._delete_recursive(root_page_id, key)

        if not result.deleted:
            # Key wasn't found, return original root
            return root_page_id

        if result.new_page_id == 0:
            # Tree is empty
            return 0

        # Check if root is now a branch with single child
        page_data = self.pager._read_page_raw(result.new_page_id)
        page_type = page_data[0]

        if page_type == 3:  # BRANCH
            branch = BranchPage.from_bytes(page_data)
            if len(branch.keys) == 0:
                # Root branch has no keys - collapse to single child
                return branch.children[0]

        return result.new_page_id

    def _delete_recursive(self, page_id: int, key: bytes) -> DeleteResult:
        """Recursively delete from subtree rooted at page_id."""
        page_data = self.pager._read_page_raw(page_id)
        page_type = page_data[0]

        if page_type == 4:  # LEAF
            leaf = LeafPage.from_bytes(page_data)
            return self._delete_leaf(leaf, key)
        elif page_type == 3:  # BRANCH
            branch = BranchPage.from_bytes(page_data)
            return self._delete_branch(branch, key)
        else:
            raise ValueError(f"Unexpected page type: {page_type}")

    def _delete_leaf(self, leaf: LeafPage, key: bytes) -> DeleteResult:
        """Delete from a leaf node."""
        keys = [k for k, _ in leaf.cells]
        idx = bisect.bisect_left(keys, key)

        if idx >= len(leaf.cells) or leaf.cells[idx][0] != key:
            # Key not found
            return DeleteResult(new_page_id=leaf.page_id, deleted=False)

        # Remove the key
        new_cells = list(leaf.cells)
        new_cells.pop(idx)

        if not new_cells:
            # Leaf is now empty
            return DeleteResult(new_page_id=0, deleted=True)

        # Create new leaf
        new_page_id = self.pager.allocate_page()
        new_leaf = LeafPage(page_id=new_page_id, cells=new_cells, right_sibling=leaf.right_sibling)
        self.pager.write_leaf_page(new_leaf)
        return DeleteResult(new_page_id=new_page_id, deleted=True)

    def _delete_branch(self, branch: BranchPage, key: bytes) -> DeleteResult:
        """Delete from subtree via branch node."""
        child_idx = bisect.bisect_right(branch.keys, key)
        child_page_id = branch.children[child_idx]

        result = self._delete_recursive(child_page_id, key)

        if not result.deleted:
            return DeleteResult(new_page_id=branch.page_id, deleted=False)

        new_children = list(branch.children)
        new_keys = list(branch.keys)

        if result.new_page_id == 0:
            # Child became empty - remove it from branch
            new_children.pop(child_idx)
            if child_idx > 0:
                new_keys.pop(child_idx - 1)
            elif new_keys:
                new_keys.pop(0)

            if len(new_children) == 0:
                # Branch is now empty
                return DeleteResult(new_page_id=0, deleted=True)

            if len(new_children) == 1:
                # Branch has single child - collapse
                return DeleteResult(new_page_id=new_children[0], deleted=True)
        else:
            # Child still exists, just update pointer
            new_children[child_idx] = result.new_page_id

        # Create new branch
        new_page_id = self.pager.allocate_page()
        new_branch = BranchPage(page_id=new_page_id, keys=new_keys, children=new_children)
        self.pager.write_branch_page(new_branch)
        return DeleteResult(new_page_id=new_page_id, deleted=True)

    def range_scan(
        self, root_page_id: int, start: bytes | None = None, end: bytes | None = None
    ) -> Iterator[tuple[bytes, bytes]]:
        """Iterate over key-value pairs in sorted order using cursor stack.

        Uses a stack-based cursor to traverse leaves without relying on sibling
        pointers. This avoids the stale pointer problem inherent in CoW trees
        and provides consistent O(n) performance without cache warm-up.

        The cursor stack tracks the path from root to current position. When a
        leaf is exhausted, we pop up the stack to find the next subtree - this
        is amortized O(1) per element since each node is pushed/popped once.

        Args:
            root_page_id: Root of the tree to scan
            start: Inclusive start key (None = from beginning)
            end: Exclusive end key (None = to end)

        Yields:
            (key, value) tuples in sorted key order
        """
        if root_page_id == 0:
            return

        # Stack of (branch_page, child_index) - tracks path for finding next leaf
        stack: list[tuple[BranchPage, int]] = []

        # Traverse to starting leaf, building the stack
        page_id = root_page_id
        while True:
            page_data = self.pager._read_page_raw(page_id)
            page_type = page_data[0]

            if page_type == 4:  # LEAF
                leaf = LeafPage.from_bytes(page_data)
                break
            elif page_type == 3:  # BRANCH
                branch = BranchPage.from_bytes(page_data)
                if start is None:
                    child_idx = 0
                else:
                    child_idx = bisect.bisect_right(branch.keys, start)
                stack.append((branch, child_idx))
                page_id = branch.children[child_idx]
            else:
                raise ValueError(f"Unexpected page type: {page_type}")

        # Iterate through all leaves using stack for navigation
        while True:
            # Yield matching keys from current leaf
            for key, value in leaf.cells:
                if start is not None and key < start:
                    continue
                if end is not None and key >= end:
                    return
                yield (key, value)

            # Find next leaf by popping up stack until we find unexplored subtree
            next_leaf = self._next_leaf_from_stack(stack)
            if next_leaf is None:
                return
            leaf = next_leaf

    def _next_leaf_from_stack(self, stack: list[tuple[BranchPage, int]]) -> LeafPage | None:
        """Find the next leaf by backtracking up the cursor stack.

        Pops the stack until finding a branch with an unexplored child,
        then traverses down to the leftmost leaf of that subtree.

        Returns None if no more leaves exist.
        """
        while stack:
            branch, child_idx = stack.pop()
            next_idx = child_idx + 1

            if next_idx < len(branch.children):
                # Found unexplored subtree - traverse down to leftmost leaf
                stack.append((branch, next_idx))
                page_id = branch.children[next_idx]

                while True:
                    page_data = self.pager._read_page_raw(page_id)
                    page_type = page_data[0]

                    if page_type == 4:  # LEAF
                        return LeafPage.from_bytes(page_data)
                    elif page_type == 3:  # BRANCH
                        branch = BranchPage.from_bytes(page_data)
                        stack.append((branch, 0))
                        page_id = branch.children[0]
                    else:
                        raise ValueError(f"Unexpected page type: {page_type}")

        return None

    def tree_height(self, root_page_id: int) -> int:
        """Return the height of the tree (0 for empty, 1 for leaf-only)."""
        if root_page_id == 0:
            return 0

        height = 0
        page_id = root_page_id

        while True:
            height += 1
            page_data = self.pager._read_page_raw(page_id)
            page_type = page_data[0]

            if page_type == 4:  # LEAF
                return height
            elif page_type == 3:  # BRANCH
                branch = BranchPage.from_bytes(page_data)
                page_id = branch.children[0]
            else:
                raise ValueError(f"Unexpected page type: {page_type}")

    def count_keys(self, root_page_id: int) -> int:
        """Count total number of keys in the tree."""
        if root_page_id == 0:
            return 0

        return sum(1 for _ in self.range_scan(root_page_id))
