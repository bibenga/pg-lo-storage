import pytest

from django.db import transaction
from warehouse.storage import DbFileStorage, DbFileIO


@pytest.mark.django_db(transaction=True)
class TestDbFileIO:
    def test_create_and_write(self):
        with transaction.atomic():
            with DbFileIO(None, 0, "wb") as f:
                f.write(b'aa')
                assert f.loid is not None

            with transaction.get_connection().cursor() as cursor:
                cursor.execute("select count(loid) from pg_largeobject where loid=%s", [f.loid])
                assert cursor.fetchone()[0] == 1

    def test_write_and_read(self):
        with transaction.atomic():
            with DbFileIO(None, 0, "wb") as w:
                w.write(b'ab')
                assert w.loid is not None

            with DbFileIO(None, w.loid, "rb") as r:
                assert r.read(1) == b'a'
                assert r.read(1) == b'b'
                assert r.read(1) == None

    def test_tell(self):
        with transaction.atomic():
            with DbFileIO(None, 0, "wb") as w:
                w.write(b'ab')
                assert w.loid is not None

            with DbFileIO(None, w.loid, "rb") as r:
                assert r.tell() == 0
                assert r.read(1) == b'a'
                assert r.tell() == 1
                assert r.read(1) == b'b'
                assert r.tell() == 2
                assert r.read(1) == None
                assert r.tell() == 2

    def test_size(self):
        with transaction.atomic():
            with DbFileIO(None, 0, "wb") as w:
                w.write(b'abcd')
                assert w.size == 4
                assert w.tell() == 4

                assert w.seek(2)
                assert w.size == 4
                assert w.tell() == 2

    def test_writelines(self):
        with transaction.atomic():
            with DbFileIO(None, 0, "wb") as w:
                w.writelines([b'ab\n', b'cd\n'])
                assert w.loid is not None
                assert w.size == 6

    def test_readline(self):
        with transaction.atomic():
            with DbFileIO(None, 0, "wb") as w:
                w.write(b'abcd\nef')
                assert w.loid is not None

            with DbFileIO(None, w.loid, "rb") as r:
                assert r.readline(3) == b'abc'
                assert r.tell() == 3

                r.seek(0)
                assert r.readline() == b'abcd\n'
                assert r.tell() == 5

    def test_readlines(self):
        with transaction.atomic():
            with DbFileIO(None, 0, "wb") as w:
                w.write(b'ab\ncd\n')
                assert w.loid is not None

            with DbFileIO(None, w.loid, "rb") as r:
                lines = r.readlines()
                assert lines == [b'ab\n', b'cd\n']
                assert r.tell() == 6

    def test_iter(self):
        with transaction.atomic():
            with DbFileIO(None, 0, "wb") as w:
                w.write(b'ab\ncd\ne')
                assert w.loid is not None

            with DbFileIO(None, w.loid, "rb") as r:
                assert next(r) == b'ab\n'
                assert r.tell() == 3
                assert next(r) == b'cd\n'
                assert r.tell() == 6
                assert next(r) == b'e'
                assert r.tell() == 7


@pytest.mark.django_db(transaction=True)
class TestDbFileStorage:
    @pytest.fixture
    def storage(self) -> DbFileStorage:
        storage = DbFileStorage()
        return storage
