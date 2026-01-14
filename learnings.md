# Learning Notes

## Checkpoint: Stage 3 - Copy-on-Write B+ Tree
- What we added: `storage/btree.py` with full CoW B+ tree supporting insert, get, delete, and range_scan operations
- Edge case to remember: Splits must be detected by calculating exact byte size needed, not by checking `available_space() >= 0` (which clips to 0)
- How to inspect it: `uv run python tools/db_inspect.py --db ./tmp/dev.db --tree`
- What I'd improve next: Implement leaf node merging on delete (currently leaves can become sparse)

## Design Decision: Sibling Pointers in CoW B+ Trees

Traditional B+ trees maintain sibling pointers between leaf nodes for O(1) sequential access during range scans. In a CoW tree, this creates a challenge: when a leaf is modified, its predecessor's sibling pointer becomes stale (points to the old version).

**Three approaches we considered:**

| Approach | Write Cost | First Scan | Subsequent Scans | Complexity |
|----------|------------|------------|------------------|------------|
| In-order traversal | O(log n) | O(n log n) | O(n log n) | Low |
| Cascading updates | O(n) | O(n) | O(n) | Medium |
| Lazy repair + cache | O(log n) | O(n log n) | O(n) | Medium |

**1. In-order traversal (no sibling pointers)**
- Range scan recursively traverses the tree
- Simple but re-traverses from root for each subtree
- Good baseline for read-light workloads

**2. Cascading sibling updates**
- When leaf L is copied to L', update predecessor's sibling to point to L'
- But updating predecessor creates a new page, requiring its predecessor to update... cascade!
- Result: O(n) write amplification per insert (we measured ~5 pages/insert, test suite went from 4s to 88s)
- Trade-off: Fast reads, slow writes

**3. Lazy repair with caching (current implementation)**
- Sibling pointers set during splits only (left → right is always correct)
- On range scan cache miss: traverse tree to find correct next leaf, cache result
- On cache hit: O(1) leaf-to-leaf access
- Trade-off: First scan pays traversal cost, subsequent scans are fast
- Best for read-heavy workloads with repeated scans on same snapshot

**Why lazy repair wins for key-value stores:**
- Most KV workloads are read-heavy (90%+ reads)
- MVCC means readers hold snapshots—same root scanned multiple times
- Write performance matters for ingestion; can't afford O(n) per insert
- Cache naturally warms up during normal operation
