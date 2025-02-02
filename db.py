NOT_FOUND = "NULL"


class ParrotDB:
    def __init__(self):
        self.data = {}
        self.transactions = []

    def set(self, key: str, value: str) -> None:
        """
        Stores a key with the specified value. Currently just supports strings
        """
        try:
            self.data[key] = value
        except KeyError:
            return None

    def get(self, key: str) -> str | None:
        """Retrieve the value associated with the key. If key doesn't exist, it'll return Null"""
        return self.data.get(key, NOT_FOUND)

    def delete(self, key: str) -> str | None:
        """Removes the key from the database. If key does not exist, return Null"""
        try:
            del self.data[key]
        except KeyError:
            return NOT_FOUND

    def count(self, value: str) -> int:
        """Return the count of keys which have a certain value"""
        return sum(1 for _, v in self.data.items() if v == value)

    def clear(self) -> None:
        """Reset databases state to initial state"""
        self.data = {}
        self.transactions = []

    def begin(self) -> None:
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

