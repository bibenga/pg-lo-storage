import csv
import io
import pytest
from django.db import transaction

from pg_lo_storage.storage import DbFileIO


@pytest.mark.django_db(transaction=True)
class TestDbFileIO:
    @transaction.atomic
    def test_create(self):
        with DbFileIO(0, "wb") as f:
            f.write(b"aa")
            assert f.loid is not None
            assert f.name == str(f.loid)

        with transaction.get_connection().cursor() as cursor:
            cursor.execute("select count(loid) from pg_largeobject where loid=%s", [f.loid])
            assert cursor.fetchone()[0] == 1

    @transaction.atomic
    def test_write(self):
        with DbFileIO(0, "wb") as w:
            w.write(b"ab")
            assert w.loid is not None

        with DbFileIO(w.loid, "rb") as r:
            assert r.read(1) == b"a"
            assert r.read(1) == b"b"
            assert r.read(1) == b""

    @transaction.atomic
    def test_tell(self):
        with DbFileIO(0, "wb") as w:
            w.write(b"ab")
            assert w.loid is not None

        with DbFileIO(w.loid, "rb") as r:
            assert r.tell() == 0
            assert r.read(1) == b"a"
            assert r.tell() == 1
            assert r.read(1) == b"b"
            assert r.tell() == 2
            assert r.read(1) == b""
            assert r.tell() == 2

    @transaction.atomic
    def test_size(self):
        with DbFileIO(0, "wb") as w:
            w.write(b"abcd")
            assert w.size == 4
            assert w.tell() == 4

            assert w.seek(2)
            assert w.size == 4
            assert w.tell() == 2

    @transaction.atomic
    def test_writelines(self):
        with DbFileIO(0, "wb") as w:
            w.writelines([b"ab\n", b"cd\n"])
            assert w.loid is not None
            assert w.size == 6

    @transaction.atomic
    def test_readline(self):
        with DbFileIO(0, "wb") as w:
            w.write(b"abcd\nef")
            assert w.loid is not None

        with DbFileIO(w.loid, "rb") as r:
            assert r.readline(3) == b"abc"
            assert r.tell() == 3

            r.seek(0)
            assert r.readline() == b"abcd\n"
            assert r.tell() == 5

    @transaction.atomic
    def test_readlines(self):
        with DbFileIO(0, "wb") as w:
            w.write(b"ab\ncd\n")
            assert w.loid is not None

        with DbFileIO(w.loid, "rb") as r:
            lines = r.readlines()
            assert lines == [b"ab\n", b"cd\n"]
            assert r.tell() == 6

    @transaction.atomic
    def test_iter(self):
        with DbFileIO(0, "wb") as w:
            w.write(b"ab\ncd\ne")
            assert w.loid is not None

        with DbFileIO(w.loid, "rb") as r:
            assert next(r) == b"ab\n"
            assert r.tell() == 3
            assert next(r) == b"cd\n"
            assert r.tell() == 6
            assert next(r) == b"e"
            assert r.tell() == 7

    @transaction.atomic
    def test_csv(self):
        with DbFileIO(0, "wb") as csvfile:
            fieldnames = ["first_name", "last_name"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({"first_name": "Baked", "last_name": "Beans"})
            writer.writerow({"first_name": "Lovely", "last_name": "Spam"})
            writer.writerow({"first_name": "Wonderful", "last_name": "Spam"})

        loid = csvfile.loid

        with DbFileIO(loid, "rb") as bcsvfile , io.TextIOWrapper(bcsvfile, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            assert len(rows) == 3
