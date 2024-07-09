import pytest
from django.db import transaction

from pg_lo_storage.storage import MODE_READWRITE, SEEK_END, SEEK_SET


@pytest.mark.django_db(transaction=True)
class TestLargeObject:
    def test_lo(self, mocker):
        # https://www.postgresql.org/docs/16/lo-funcs.html
        # https://www.postgresql.org/docs/current/lo-interfaces.html
        with transaction.atomic():
            conn = transaction.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("select distinct loid from pg_largeobject")
                loids = [row for row in cursor.fetchall()]
                print(loids)
                assert loids == []

                cursor.execute("select lo_create(0) as loid")
                row = cursor.fetchone()
                print(row)
                loid = row[0]
                assert loid != 0

                cursor.execute("select lo_put(%s, 2, %s)", [loid, "olala".encode()])
                row = cursor.fetchone()
                print(row)

                # cursor.execute("select lo_from_bytea(0, %s)", ["olala".encode()])
                # row = cursor.fetchone()
                # print(row)
                # loid = row[0]

                cursor.execute("select * from pg_largeobject")
                loids = [row[0] for row in cursor.fetchall()]
                assert len(loids) == 1
                print(loids)

                # just read
                cursor.execute("select lo_get(%s)", [loid])
                row = cursor.fetchone()
                print(row)

                # work like with file
                cursor.execute("select lo_open(%s, %s)", [loid, MODE_READWRITE])
                row = cursor.fetchone()
                print(row)
                fd = row[0]

                cursor.execute("select lo_lseek64(%s, 0, %s)", [fd, SEEK_END])
                row = cursor.fetchone()
                print(row)
                cursor.execute("select lo_tell64(%s)", [fd])
                row = cursor.fetchone()
                print(row)

                cursor.execute("select lo_lseek64(%s, 0, %s)", [fd, SEEK_SET])
                row = cursor.fetchone()
                print(row)
                cursor.execute("select loread(%s, 0)", [fd])
                row = cursor.fetchone()
                print(row)

                cursor.execute("select lo_truncate64(%s, 2)", [fd])
                row = cursor.fetchone()
                print(row)

                cursor.execute("select lo_close(%s)", [fd])
                row = cursor.fetchone()
                print(row)

                cursor.execute("select lo_unlink(%s)", [loid])
                row = cursor.fetchone()
                print(row)
