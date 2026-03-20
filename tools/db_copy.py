#!/usr/bin/env python3
"""Database copy tool with optional compaction.

Similar to LMDB's mdb_copy, this tool copies a database to a new file.
With the -c flag, it compacts the database by writing only live pages
contiguously, eliminating holes from deleted data.

Usage:
    uv run python tools/db_copy.py source.db dest.db        # Raw copy
    uv run python tools/db_copy.py source.db dest.db -c     # Compact copy
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from parrot_db import ParrotDB


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Copy a ParrotDB database file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Copy database preserving layout
    %(prog)s ./data.db ./backup.db

    # Copy with compaction (removes holes, reduces file size)
    %(prog)s ./data.db ./compact.db -c
""",
    )
    parser.add_argument("source", help="Source database file")
    parser.add_argument("dest", help="Destination database file")
    parser.add_argument(
        "-c",
        "--compact",
        action="store_true",
        help="Compact the database (rewrite contiguously, removes holes)",
    )

    args = parser.parse_args()

    source_path = Path(args.source)
    dest_path = Path(args.dest)

    if not source_path.exists():
        print(f"Error: Source file not found: {source_path}", file=sys.stderr)
        return 1

    if dest_path.exists():
        print(f"Error: Destination file already exists: {dest_path}", file=sys.stderr)
        return 1

    try:
        # Get source file size for comparison
        source_size = source_path.stat().st_size

        with ParrotDB(source_path, create=False) as db:
            db.copy(dest_path, compact=args.compact)

        dest_size = dest_path.stat().st_size

        mode = "compacted" if args.compact else "copied"
        print(f"Database {mode}: {source_path} -> {dest_path}")
        print(f"Source size:      {source_size:,} bytes")
        print(f"Destination size: {dest_size:,} bytes")

        if args.compact and dest_size < source_size:
            saved = source_size - dest_size
            pct = (saved / source_size) * 100
            print(f"Space saved:      {saved:,} bytes ({pct:.1f}%)")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
