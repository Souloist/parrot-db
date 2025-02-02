import pytest

from db import ParrotDB
from exceptions import KeyNotFound, NoActiveTransactions


@pytest.fixture
def db():
    db_instance = ParrotDB()
    yield db_instance
    db_instance.clear()


def test_add_key(db) -> None:
    db.set("name", "Jamie")
    assert db.get("name") == "Jamie"


def test_get_missing_key(db) -> None:
    with pytest.raises(KeyNotFound):
        db.get("Something")


def test_remove_key(db) -> None:
    db.set("name", "Jamie")
    db.delete("name")
    with pytest.raises(KeyNotFound):
        db.delete("name")


def test_count_values(db) -> None:
    for key, value in [("1", "happy"), ("2", "sad"), ("3", "happy"), ("4", "happy")]:
        db.set(key, value)

    assert db.count("happy") == 3


def test_begin_transaction(db) -> None:
    db.set("name", "richard")
    db.begin()
    db.set("name", "not richard")
    assert db.get("name") == "not richard"


def test_rollback_transaction(db) -> None:
    db.set("name", "richard")
    db.begin()
    db.set("name", "not richard")
    assert db.count("not richard") == 1
    db.rollback()
    assert db.get("name") == "richard"

    with pytest.raises(NoActiveTransactions):
        db.rollback()


def test_commit_transaction(db) -> None:
    db.set("name", "richard")
    db.begin()
    db.set("name", "not richard")
    db.commit()
    assert db.get("name") == "not richard"
    assert not db.count("richard")

    with pytest.raises(NoActiveTransactions):
        db.commit()


def test_nested_transactions(db) -> None:
    db.set("name", "richard")
    db.begin()
    db.set("name", "not richard")
    assert db.get("name") == "not richard"

    db.begin()
    db.set("name", "something else")
    assert db.get("name") == "something else"

    db.rollback()
    assert db.get("name") == "not richard"

    db.commit()
    assert db.get("name") == "not richard"
