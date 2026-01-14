# Learning Notes

## Checkpoint: Stage 3 - Copy-on-Write B+ Tree
- What we added: `storage/btree.py` with full CoW B+ tree supporting insert, get, delete, and range_scan operations
- Edge case to remember: Splits must be detected by calculating exact byte size needed, not by checking `available_space() >= 0` (which clips to 0)
- How to inspect it: `uv run python tools/db_inspect.py --db ./tmp/dev.db --tree`
- What I'd improve next: Implement leaf node merging on delete (currently leaves can become sparse)

## Design Decision: Range Scans in CoW B+ Trees

Traditional B+ trees maintain sibling pointers between leaf nodes for O(1) sequential access during range scans. In a CoW tree, this creates a fundamental tension: when a leaf is modified, its predecessor's sibling pointer becomes stale (points to the old version).

This is a well-known problem. Rodeh's work on CoW-friendly B-trees (which influenced btrfs) explicitly discusses avoiding this "ripple effect" where updating one leaf forces updates to propagate through neighbors.

### Approaches Considered

| Approach | Write Cost | Range Scan | Cache Dependent | Memory |
|----------|------------|------------|-----------------|--------|
| Cascading sibling updates | O(n) | O(n) | No | O(1) |
| Lazy repair + cache | O(log n) | O(n)* | Yes | O(leaves) |
| **Cursor stack** | O(log n) | O(n) | No | O(height) |

*First scan is O(n log n), subsequent scans O(n) with warm cache

### 1. Cascading Sibling Updates

When leaf L is copied to L', update predecessor's sibling pointer to point to L'. But updating the predecessor creates a new page, which requires *its* predecessor to update, and so on.

**What we observed:**
- ~5 pages written per insert (vs ~2 for pure path copying)
- Test suite time: 4s → 88s
- Trade-off: Fast reads, unacceptably slow writes

### 2. Lazy Repair with Caching

Treat sibling pointers as "hints" rather than invariants:
- Follow sibling pointer optimistically
- Validate it points to correct page (check key ordering, page type)
- On stale pointer: traverse tree to find correct next leaf, cache result
- Subsequent scans use cache for O(1) leaf transitions

**Problems:**
- First scan still pays O(n log n) traversal cost
- Cache is process-local and non-persistent—loses benefit on restart
- Validation is tricky: old sibling may still have valid-looking keys
- If page IDs are reused, stale pointers could be dangerous without generation checks

### 3. Cursor Stack (Current Implementation)

Don't use sibling pointers at all. Instead, maintain a stack of `(branch_node, child_index)` as we traverse:

```
Stack: [(root, 2), (branch_A, 1), (branch_B, 3)]
         ↓
      Current leaf
```

When we exhaust a leaf:
1. Pop the stack: `(branch_B, 3)`
2. Try next child: `branch_B.children[4]`
3. If exists, traverse down to leftmost leaf of that subtree
4. If not, pop again and repeat

**Why this wins:**
- **No cache dependency** - every scan is O(n), not just warm ones
- **No stale pointer risk** - we follow the actual tree structure
- **Amortized O(1) per element** - each node pushed/popped exactly once
- **O(height) memory** - just the stack, not O(leaves) for cache
- **Simpler** - no validation logic, no hint semantics

**Performance characteristics:**
- Write: O(log n) pure path copying
- Range scan: O(n + height) total, amortized O(1) per key
- Memory: O(height) during iteration (~3-4 stack frames typically)

### Why CoW Trees Avoid Leaf Chaining

The cursor stack approach aligns with how production CoW-friendly trees handle this:

> "The leaves being linked causes the ripple effect that can force rewriting large portions of the tree."

Most serious CoW implementations either:
1. Don't have sibling links at all (use cursor/stack)
2. Use indirection or extra structures to avoid the ripple
3. Treat links as hints with generation-based validation

We chose option 1 for simplicity and correctness.

### Sibling Pointers as Optional Hints

The `right_sibling` field still exists in `LeafPage` and is set correctly during splits (left → right). A future optimization could use it as a hint:

```python
def _next_leaf_from_stack_with_hint(self, stack, sibling_hint):
    if sibling_hint != 0 and self._validate_sibling(sibling_hint):
        return sibling_hint  # Fast path
    return self._next_leaf_from_stack(stack)  # Fallback
```

But this adds complexity for marginal benefit—the stack approach is already O(1) amortized.
