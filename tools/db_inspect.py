#!/usr/bin/env python3
"""Database inspection tool for debugging and learning.

Usage:
    uv run python tools/db_inspect.py --db ./tmp/dev.db --summary
    uv run python tools/db_inspect.py --db ./tmp/dev.db --page 3
    uv run python tools/db_inspect.py --db ./tmp/dev.db --tree
    uv run python tools/db_inspect.py --db ./tmp/dev.db --freelist
"""

import argparse
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from models.storage import PageHeader, PageType
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
    LeafPage,
)


def print_header(pager: Pager) -> None:
    """Print header page information."""
    header = pager.read_header()
    print("=== Header Page (Page 0) ===")
    print(f"  Magic: {header.magic!r}")
    print(f"  Version: {header.version}")
    print(f"  Page Size: {header.page_size} bytes")
    print()


def print_meta_pages(pager: Pager) -> None:
    """Print both meta pages and indicate which is active."""
    print("=== Meta Pages ===")

    for page_id in (META_PAGE_0_ID, META_PAGE_1_ID):
        try:
            meta = pager.read_meta_page(page_id, verify_checksum=True)
            active = pager.read_active_meta()
            is_active = meta.page_id == active.page_id

            status = " (ACTIVE)" if is_active else " (inactive)"
            print(f"  Meta Page {page_id - 1}{status}:")
            print(f"    txn_id: {meta.txn_id}")
            print(f"    root_page_id: {meta.root_page_id}")
            print(f"    freelist_page_id: {meta.freelist_page_id}")
        except ValueError as e:
            print(f"  Meta Page {page_id - 1}: INVALID ({e})")
    print()


def print_summary(pager: Pager) -> None:
    """Print database summary."""
    from storage.btree import BTree

    print("=" * 50)
    print("DATABASE SUMMARY")
    print("=" * 50)
    print()

    print_header(pager)
    print_meta_pages(pager)

    print("=== File Statistics ===")
    print(f"  Total Pages: {pager.page_count}")
    print(f"  Data Pages: {pager.page_count - FIRST_DATA_PAGE_ID}")
    print(f"  Free Pages: {pager.freelist.count()}")
    print(f"  File Size: {pager.page_count * pager.page_size} bytes")
    print()

    # Tree statistics
    meta = pager.read_active_meta()
    if meta.root_page_id != 0:
        btree = BTree(pager)
        print("=== B+ Tree Statistics ===")
        print(f"  Root Page: {meta.root_page_id}")
        print(f"  Tree Height: {btree.tree_height(meta.root_page_id)}")
        print(f"  Total Keys: {btree.count_keys(meta.root_page_id)}")
        print()


def print_page(pager: Pager, page_id: int) -> None:
    """Print details of a specific page."""
    if page_id == HEADER_PAGE_ID:
        print_header(pager)
        return

    if page_id in (META_PAGE_0_ID, META_PAGE_1_ID):
        meta = pager.read_meta_page(page_id)
        print(f"=== Meta Page {page_id - 1} (Page {page_id}) ===")
        print(f"  txn_id: {meta.txn_id}")
        print(f"  root_page_id: {meta.root_page_id}")
        print(f"  freelist_page_id: {meta.freelist_page_id}")
        return

    # Read page header to determine type
    data = pager.read_page_raw(page_id)
    header = PageHeader.from_bytes(data)

    print(f"=== Page {page_id} ===")
    print(f"  Type: {header.page_type.name}")
    print(f"  Checksum: 0x{header.checksum:08X}")

    match header.page_type:
        case PageType.FREELIST:
            page = FreelistPage.from_bytes(data)
            print(f"  Free Page Count: {len(page.free_page_ids)}")
            if page.free_page_ids:
                print(f"  Free Pages: {page.free_page_ids[:20]}{'...' if len(page.free_page_ids) > 20 else ''}")

        case PageType.LEAF:
            page = LeafPage.from_bytes(data)
            print(f"  Cell Count: {len(page.cells)}")
            print(f"  Right Sibling: {page.right_sibling}")
            for i, (key, value) in enumerate(page.cells[:10]):
                key_repr = key[:20].hex() + ("..." if len(key) > 20 else "")
                value_repr = value[:20].hex() + ("..." if len(value) > 20 else "")
                print(f"    [{i}] key={key_repr} value={value_repr}")
            if len(page.cells) > 10:
                print(f"    ... and {len(page.cells) - 10} more cells")

        case PageType.BRANCH:
            page = BranchPage.from_bytes(data)
            print(f"  Key Count: {len(page.keys)}")
            print(f"  Children: {page.children}")
            for i, key in enumerate(page.keys[:10]):
                key_repr = key[:20].hex() + ("..." if len(key) > 20 else "")
                print(f"    [{i}] separator={key_repr}")
            if len(page.keys) > 10:
                print(f"    ... and {len(page.keys) - 10} more keys")

        case _:
            print("  (Unknown page type)")

    print()


def print_freelist(pager: Pager) -> None:
    """Print freelist information."""
    print("=== Freelist ===")

    meta = pager.read_active_meta()
    if meta.freelist_page_id == 0:
        print("  No freelist page allocated")
        print(f"  In-memory free pages: {pager.freelist.count()}")
        if pager.freelist.count() > 0:
            pages = pager.freelist.to_list()
            print(f"  Free page IDs: {pages[:20]}{'...' if len(pages) > 20 else ''}")
    else:
        page = pager.read_freelist_page(meta.freelist_page_id)
        print(f"  Freelist Page ID: {meta.freelist_page_id}")
        print(f"  Free Page Count: {len(page.free_page_ids)}")
        if page.free_page_ids:
            print(f"  Free Pages: {page.free_page_ids[:20]}{'...' if len(page.free_page_ids) > 20 else ''}")
    print()


def print_tree(pager: Pager) -> None:
    """Print B+ tree structure with traversal."""
    from storage.btree import BTree

    print("=== B+ Tree Structure ===")

    meta = pager.read_active_meta()
    if meta.root_page_id == 0:
        print("  Tree is empty (no root page)")
        print()
        return

    btree = BTree(pager)
    height = btree.tree_height(meta.root_page_id)
    key_count = btree.count_keys(meta.root_page_id)

    print(f"  Root Page ID: {meta.root_page_id}")
    print(f"  Tree Height: {height}")
    print(f"  Total Keys: {key_count}")
    print()

    # Print tree structure with indentation
    print("  Tree Layout:")
    _print_tree_node(pager, meta.root_page_id, depth=0)
    print()


def _print_tree_node(pager: Pager, page_id: int, depth: int) -> None:
    """Recursively print tree node information."""
    indent = "    " + "  " * depth
    data = pager.read_page_raw(page_id)
    page_type = data[0]

    if page_type == 4:  # LEAF
        leaf = LeafPage.from_bytes(data)
        print(f"{indent}[Leaf {page_id}] {len(leaf.cells)} cells")
        if leaf.cells and depth < 3:
            first_key = leaf.cells[0][0]
            last_key = leaf.cells[-1][0]
            first_repr = _key_repr(first_key)
            last_repr = _key_repr(last_key)
            print(f"{indent}  keys: {first_repr} .. {last_repr}")
    elif page_type == 3:  # BRANCH
        branch = BranchPage.from_bytes(data)
        print(f"{indent}[Branch {page_id}] {len(branch.keys)} keys, {len(branch.children)} children")
        if branch.keys and depth < 3:
            first_sep = _key_repr(branch.keys[0])
            last_sep = _key_repr(branch.keys[-1])
            print(f"{indent}  separators: {first_sep} .. {last_sep}")
        # Recursively print children (limit depth to avoid huge output)
        if depth < 2:
            for child_id in branch.children:
                _print_tree_node(pager, child_id, depth + 1)
        elif depth == 2:
            print(f"{indent}  ({len(branch.children)} children not expanded)")


def _key_repr(key: bytes, max_len: int = 20) -> str:
    """Format a key for display."""
    try:
        text = key.decode("utf-8")
        if len(text) > max_len:
            return repr(text[:max_len] + "...")
        return repr(text)
    except UnicodeDecodeError:
        hex_str = key[:max_len].hex()
        if len(key) > max_len:
            return f"0x{hex_str}..."
        return f"0x{hex_str}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect parrot-db database files")
    parser.add_argument("--db", required=True, help="Path to database file")
    parser.add_argument("--summary", action="store_true", help="Show database summary")
    parser.add_argument("--page", type=int, help="Show specific page")
    parser.add_argument("--tree", action="store_true", help="Show B+ tree structure")
    parser.add_argument("--freelist", action="store_true", help="Show freelist")

    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: Database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    with Pager(db_path, create=False) as pager:
        if args.summary:
            print_summary(pager)
        elif args.page is not None:
            print_page(pager, args.page)
        elif args.tree:
            print_tree(pager)
        elif args.freelist:
            print_freelist(pager)
        else:
            print_summary(pager)


if __name__ == "__main__":
    main()
