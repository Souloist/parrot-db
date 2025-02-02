import pytest

from db import ParrotDB


@pytest.fixture
def db():
    db_instance = ParrotDB()
    yield db_instance
    db_instance.clear()


def test_add_key(db) -> None:
    db.set("name", "Jamie")
    assert db.get("name") == "Jamie"


def test_remove_key(db) -> None:
    db.set("name", "Jamie")
    db.delete("name")
    assert not db.get("name")

