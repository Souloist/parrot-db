from exceptions import KeyNotFound, NoActiveTransactions


class ParrotDB:
    def __init__(self):
        self._data = {}
        self._transactions = []

    def _get_scope(self):
        if self._transactions:
            return self._transactions[-1]
        else:
            return self._data

    def set(self, key: str, value: str) -> None:
        """
        Stores a key with the specified value. Currently just supports strings
        """
        data = self._get_scope()
        data[key] = value

    def get(self, key: str) -> str:
        """Retrieve the value associated with the key. If key doesn't exist, a KeyNotFound exception will be raised"""
        data = self._get_scope()
        try:
            return data[key]
        except KeyError:
            raise KeyNotFound

    def delete(self, key: str) -> None:
        """Removes the key from the database. If key does not exist, a KeyNotFound exception will be raised"""
        data = self._get_scope()
        try:
            del data[key]
        except KeyError:
            raise KeyNotFound

    def count(self, value: str) -> int:
        """Return the count of keys which have a certain value"""
        data = self._get_scope()
        return sum(1 for _, v in data.items() if v == value)

    def clear(self) -> None:
        """Reset databases state to initial state"""
        self._data = {}
        self._transactions = []

    def begin(self) -> None:
        self._transactions.append(self._data.copy())

    def commit(self) -> None:
        """
        commit currently replaces the next oldest snapshot in the transaction stack with
        the current snapshot. If no snapshot exists, then it replaces the current _data

        TODO: keep track of versions in keys and only update if the versions are less than the
        current version
        """
        if self._transactions:
            latest_snapshot = self._transactions.pop()
            if self._transactions:
                self._transactions[-1] = latest_snapshot
            else:
                self._data = latest_snapshot
        else:
            raise NoActiveTransactions

    def rollback(self) -> None:
        """
        Rollback the current transaction by discarding the latest snapshot.
        """
        try:
            self._transactions.pop()
        except IndexError:
            raise NoActiveTransactions
