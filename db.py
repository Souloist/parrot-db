import threading
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass

from exceptions import KeyNotFound, NoActiveTransactions


@dataclass
class Transaction:
    current_version: int
    data: defaultdict

    def get_next_version(self):
        self.current_version += 1
        return self.current_version


class ParrotDB:
    def __init__(self):
        self._data = defaultdict(list)
        self._current_version = 0

        #  txn_id -> snapshot
        self._transactions = defaultdict(list)

        self._lock = threading.Lock()

    def _increment_version(self):
        with self._lock:
            self._current_version += 1
            return self._current_version

    def _get_current_transaction(self, txn_id):
        if txn_id in self._transactions:
            # If snapshots exist for a txn_id, return the most recent one
            return self._transactions[txn_id][-1]

    def set(self, key: str, value: str) -> None:
        """
        Stores a key with the specified value. Currently just supports strings
        """
        txn_id = threading.get_ident()
        live_transaction = self._get_current_transaction(txn_id)
        if live_transaction:
            version = live_transaction.get_next_version()
            live_transaction.data[key].append((version, value))
        else:
            version = self._increment_version()
            self._data[key].append((version, value))

    def get(self, key: str) -> str:
        """Retrieve the value associated with the key. If key doesn't exist, a KeyNotFound exception will be raised"""
        txn_id = threading.get_ident()
        live_transaction = self._get_current_transaction(txn_id)

        try:
            if live_transaction:
                return live_transaction.data[key][-1][1]
            else:
                return self._data[key][-1][1]
        except (KeyError, IndexError):
            raise KeyNotFound

    def delete(self, key: str) -> None:
        """Removes the key from the database. If key does not exist, a KeyNotFound exception will be raised"""
        txn_id = threading.get_ident()
        live_transaction = self._get_current_transaction(txn_id)

        try:
            if live_transaction:
                del live_transaction.data[key]
            else:
                del self._data[key]
        except (KeyError, IndexError):
            raise KeyNotFound

    def count(self, value: str) -> int:
        """Return the count of keys which have a certain value"""
        txn_id = threading.get_ident()
        live_transaction = self._get_current_transaction(txn_id)

        data = live_transaction.data if live_transaction else self._data
        return sum(1 for v in data.values() if v[-1][1] == value)

    def clear(self) -> None:
        """Reset databases state to initial state"""
        self._data = defaultdict(list)
        self._transactions = {}
        self._current_version = 0

    def begin(self) -> None:
        txn_id = threading.get_ident()
        self._transactions[txn_id].append(Transaction(self._current_version, deepcopy(self._data)))

    def commit(self) -> None:
        """
        commit currently replaces the next oldest snapshot in the transaction stack with
        the current snapshot. If no snapshot exists, then it replaces the current _data

        TODO: keep track of versions in keys and only update if the versions are less than the
        current version
        """
        txn_id = threading.get_ident()

        if txn_id not in self._transactions:
            raise NoActiveTransactions()

        latest_transaction = self._transactions[txn_id].pop()
        if self._transactions[txn_id]:
            self._transactions[txn_id][-1] = latest_transaction
        else:
            del self._transactions[txn_id]

            # TODO: don't override but check on if version numbers are higher for snapshot
            self._data = latest_transaction.data

    def rollback(self) -> None:
        """
        Rollback the current transaction by discarding the latest snapshot.
        If no transactions are active, raise NoActiveTransactions.
        """
        txn_id = threading.get_ident()

        if txn_id not in self._transactions:
            raise NoActiveTransactions()

        self._transactions[txn_id].pop()

        if not self._transactions[txn_id]:
            del self._transactions[txn_id]

    def show_state(self):
        """Helper function to return state, current version and current active transactions"""
        return {"data": self._data, "current_version": self._current_version, "active_transactions": self._transactions}
