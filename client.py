from db import ParrotDB
from exceptions import KeyNotFound, NoActiveTransactions


def main():
    db = ParrotDB()
    prompt = """
    Welcome to Parrot database!
    Commands:
        set <key> <value>   - Sets the value for the given key
        get <key>           - Returns the value for the given key
        count <value>       - Returns number of keys with given value
        delete <key>        - Deletes key
        exit                - Exits the program
        
        begin               - Begins a transaction. Supported nested transactions
        commit              - Commits current transaction
        rollback            - Rollback current transaction
    """
    print(prompt)

    while True:
        try:
            command = input("> ").strip()
            if command.lower() == "exit":
                print("Exiting...")
                break

            parts = command.split()
            action = parts[0].lower()
            key = parts[1] if len(parts) > 1 else None

            if action == "set":
                if len(parts) > 3:
                    print("Invalid command. Use set <key> <value>.")
                    continue
                value = parts[2]
                db.set(key, value)
                print(f"Set {key} to {value}")
            elif action == "get":
                value = db.get(key)
                if value is not None:
                    print(f"Get {key}: {value}")
                else:
                    print(f"No value for {key}.")
            elif action == "delete":
                if len(parts) > 3:
                    print("Invalid command. Use set <key> <value>.")
                    continue
                try:
                    db.delete(key)
                    print(f"Deleted {key}")
                except KeyNotFound:
                    print(f"Key not found")
            elif action == "begin":
                db.begin()
            elif action == "commit":
                try:
                    db.commit()
                except NoActiveTransactions:
                    print(f"Cannot commit with no active transactions")
            elif action == "rollback":
                try:
                    db.rollback()
                except NoActiveTransactions:
                    print(f"Cannot rollback with no active transactions")
            else:
                print("Unknown command.")
        except KeyboardInterrupt:
            print("\nExiting...")
            break


if __name__ == '__main__':
    main()
