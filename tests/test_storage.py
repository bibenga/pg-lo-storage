import io
import re

import pytest
from django.db import transaction

from pg_lo_storage.storage import DbFileStorage


@pytest.mark.django_db(transaction=True)
class TestStorage:
    @pytest.fixture
    def storage(self) -> DbFileStorage:
        storage = DbFileStorage()
        return storage

    @transaction.atomic
    def test_open(self, storage: DbFileStorage):
        with pytest.raises(FileNotFoundError):
            storage.open("0.bin", "wb")

    @transaction.atomic
    def test_save(self, storage: DbFileStorage):
        data = io.BytesIO(b"abcd")
        name = storage.save("olala.bin", data)
        assert re.match("\d+\.bin", name)

        db_file = storage.open(name)
        assert db_file.read() == b"abcd"

    @transaction.atomic
    def test_save_cmplx(self, storage: DbFileStorage):
        name = storage.save("olala.bin", io.BytesIO(b"a"))
        db_file = storage.open(name, "wb")
        db_file.seek(0, io.SEEK_END)
        db_file.write(b"b")
        db_file.write(b"cd")
        db_file.seek(0)
        assert db_file.read(3) == b"abc"
        assert db_file.read() == b"d"
