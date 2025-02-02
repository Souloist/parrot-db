from parrot import ParrotDB


def main():
    db = ParrotDB()
    prompt = """
    Welcome to Parrot database!
    Commands:
        set <key> <value>   - Sets the value for the given key
        get <key>           - Returns the value for the given key
        exit                - Exits the program
    """
    print(prompt)

    while True:
        try:
            command = input("> ").strip()
            if command.lower() == "exit":
                print("Exiting...")
                break

            parts = command.split()
            if len(parts) < 2:
                print("Invalid command.")

            action = parts[0].lower()
            key = parts[1]

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
            elif action == "unset":
                if len(parts) > 3:
                    print("Invalid command. Use set <key> <value>.")
                    continue
                db.unset(key)
                print(f"Unset {key}")
            else:
                print("Unknown command.")
        except KeyboardInterrupt:
            print("\nExiting...")
            break


if __name__ == '__main__':
    main()
