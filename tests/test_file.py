import pytest

from django.db import transaction
from warehouse.storage import PostgresqlLargeObjectStorage, PostgresqlLargeObjectFile


@pytest.mark.django_db(transaction=True)
class TestPostgresqlLargeObjectStorage:

    @pytest.fixture
    def storage(self) -> PostgresqlLargeObjectStorage:
        storage = PostgresqlLargeObjectStorage()
        return storage


@pytest.mark.django_db(transaction=True)
class TestPostgresqlLargeObjectFile:
    def test_create_and_write(self):
        with transaction.atomic():
            with PostgresqlLargeObjectFile(None, 0, "wb") as f:
                f.write(b'aa')
                assert f.loid is not None

            with transaction.get_connection().cursor() as cursor:
                cursor.execute("select count(loid) from pg_largeobject where loid=%s",[f.loid])
                assert cursor.fetchone()[0] == 1

    def test_write_and_read(self):
        with transaction.atomic():
            with PostgresqlLargeObjectFile(None, 0, "wb") as w:
                w.write(b'ab')
                assert w.loid is not None

            with PostgresqlLargeObjectFile(None, w.loid, "rb") as r:
                assert r.read(1) == b'a'
                assert r.read(1) == b'b'
                assert r.read(1) == None

    def test_tell(self):
        with transaction.atomic():
            with PostgresqlLargeObjectFile(None, 0, "wb") as w:
                w.write(b'ab')
                assert w.loid is not None

            with PostgresqlLargeObjectFile(None, w.loid, "rb") as r:
                assert r.tell() == 0
                assert r.read(1) == b'a'
                assert r.tell() == 1
                assert r.read(1) == b'b'
                assert r.tell() == 2
                assert r.read(1) == None
                assert r.tell() == 2

    def test_writelines(self):
        with transaction.atomic():
            with PostgresqlLargeObjectFile(None, 0, "wb") as w:
                w.writelines([b'ab\n', b'cd\n'])
                assert w.loid is not None
                assert w.size == 6

    def test_readlines(self):
        with transaction.atomic():
            with PostgresqlLargeObjectFile(None, 0, "wb") as w:
                w.write(b'ab\ncd\n')
                assert w.loid is not None

            with PostgresqlLargeObjectFile(None, w.loid, "rb") as r:
                assert r.tell() == 0
                assert r.readline() == b'ab\n'
                assert r.tell() == 3
                assert r.readline() == b'cd\n'
                assert r.tell() == 6
                assert r.readline() is None