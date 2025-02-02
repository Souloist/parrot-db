import pytest

from db import ParrotDB, KEY_NOT_FOUND


@pytest.fixture
def db():
    db_instance = ParrotDB()
    yield db_instance
    db_instance.clear()


def test_add_key(db) -> None:
    db.set("name", "Jamie")
    assert db.get("name") == "Jamie"


def test_get_missing_key(db) -> None:
    assert db.get("Something") == KEY_NOT_FOUND


def test_remove_key(db) -> None:
    db.set("name", "Jamie")
    db.delete("name")
    assert db.get("name") == KEY_NOT_FOUND


def test_count_values(db) -> None:
    for key, value in [("1", "happy"), ("2", "sad"), ("3", "happy"), ("4", "happy")]:
        db.set(key, value)

    assert db.count("happy") == 3


def test_begin_transaction(db) -> None:
    pass


def test_rollback_transaction(db) -> None:
    pass


def test_commit_transaction(db) -> None:
    pass


def test_begin_nested_transactions(db) -> None:
    pass


