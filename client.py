"""Interactive REPL for ParrotDB.

Provides a command-line interface for interacting with the database.
Supports transactions with begin/commit/rollback commands.
"""

import sys
from pathlib import Path

from parrot_db import ParrotDB


def main(db_path: str = "./tmp/repl.db"):
    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    db = ParrotDB(path=db_path, create=True)
    active_txn = None

    prompt = """
    Welcome to Parrot database!
    Commands:
        set <key> <value>   - Sets the value for the given key
        get <key>           - Returns the value for the given key
        delete <key>        - Deletes key
        keys [prefix]       - List all keys (optionally with prefix)
        exit                - Exits the program

        begin               - Begins a write transaction
        commit              - Commits current transaction
        rollback            - Rollback current transaction
    """
    print(prompt)
    print(f"Database: {db_path}")

    try:
        while True:
            try:
                prefix = "(txn) " if active_txn else ""
                command = input(f"{prefix}> ").strip()
                if not command:
                    continue

                if command.lower() == "exit":
                    if active_txn:
                        print("Rolling back active transaction...")
                        active_txn.rollback()
                        active_txn = None
                    print("Exiting...")
                    break

                parts = command.split(maxsplit=2)
                action = parts[0].lower()

                if action == "set":
                    if len(parts) < 3:
                        print("Usage: set <key> <value>")
                        continue
                    key = parts[1].encode()
                    value = parts[2].encode()

                    if active_txn:
                        active_txn.put(key, value)
                    else:
                        db.put(key, value)
                    print(f"Set {parts[1]} = {parts[2]}")

                elif action == "get":
                    if len(parts) < 2:
                        print("Usage: get <key>")
                        continue
                    key = parts[1].encode()

                    if active_txn:
                        value = active_txn.get(key)
                    else:
                        value = db.get(key)

                    if value is not None:
                        print(f"{parts[1]} = {value.decode()}")
                    else:
                        print("Key not found")

                elif action == "delete":
                    if len(parts) < 2:
                        print("Usage: delete <key>")
                        continue
                    key = parts[1].encode()

                    if active_txn:
                        deleted = active_txn.delete(key)
                    else:
                        deleted = db.delete(key)

                    if deleted:
                        print(f"Deleted {parts[1]}")
                    else:
                        print("Key not found")

                elif action == "keys":
                    prefix_filter = parts[1].encode() if len(parts) > 1 else None

                    if active_txn:
                        items = list(active_txn.range_scan())
                    else:
                        with db.begin() as txn:
                            items = list(txn.range_scan())

                    if prefix_filter:
                        items = [(k, v) for k, v in items if k.startswith(prefix_filter)]

                    if items:
                        for key, value in items:
                            print(f"  {key.decode()} = {value.decode()}")
                        print(f"({len(items)} keys)")
                    else:
                        print("No keys found")

                elif action == "begin":
                    if active_txn:
                        print("Transaction already active. Commit or rollback first.")
                        continue
                    active_txn = db.begin(write=True)
                    print("Transaction started")

                elif action == "commit":
                    if not active_txn:
                        print("No active transaction")
                        continue
                    active_txn.commit()
                    active_txn = None
                    print("Transaction committed")

                elif action == "rollback":
                    if not active_txn:
                        print("No active transaction")
                        continue
                    active_txn.rollback()
                    active_txn = None
                    print("Transaction rolled back")

                elif action == "help":
                    print(prompt)

                else:
                    print(f"Unknown command: {action}. Type 'help' for commands.")

            except KeyboardInterrupt:
                print()
                continue
            except Exception as e:
                print(f"Error: {e}")

    finally:
        if active_txn:
            try:
                active_txn.rollback()
            except Exception:
                pass
        db.close()


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "./tmp/repl.db"
    main(db_path)
