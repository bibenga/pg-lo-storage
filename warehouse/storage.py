import pathlib
from tempfile import SpooledTemporaryFile
from urllib.parse import urljoin

from django.conf import settings
from django.core.files.storage import Storage
from django.core.files.storage.mixins import StorageSettingsMixin
from django.core.signals import setting_changed
from django.db import connection
from django.utils.deconstruct import deconstructible
from django.utils.functional import LazyObject

MODE_WRITE = 0x20000

MODE_READ = 0x40000
MODE_READWRITE = MODE_READ | MODE_WRITE

SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

# select loid, count(pageno) from pg_largeobject group by loid


class DefaultPostgresqlLargeObjectStorage(LazyObject):
    def _setup(self):
        from warehouse.storage import PostgresqlLargeObjectStorage
        self._wrapped = PostgresqlLargeObjectStorage()


postgresql_large_object_storage: Storage = DefaultPostgresqlLargeObjectStorage()


@deconstructible(path="warehouse.storage.PostgresqlLargeObjectStorage")
class PostgresqlLargeObjectStorage(Storage, StorageSettingsMixin):
    def __init__(
        self,
        base_url=None,
        file_permissions_mode=None,
        directory_permissions_mode=None,
    ):
        self._base_url = base_url or settings.MEDIA_URL
        self._file_permissions_mode = file_permissions_mode
        self._directory_permissions_mode = directory_permissions_mode
        # self._root = InMemoryDirNode()
        # self._resolve(
        #     self.base_location, create_if_missing=True, leaf_cls=InMemoryDirNode
        # )
        setting_changed.connect(self._clear_cached_properties)

    def generate_filename(self, filename):
        # with connection.cursor() as cursor:
        #     cursor.execute("select lo_create(0) as loid")
        #     row = cursor.fetchone()
        #     loid = row[0]
        # return str(loid)
        return filename

    def get_available_name(self, filename, max_length=None):
        return filename

    def _open(self, name, mode="rb"):
        # d = FileData.objects.get(name=name)
        if "b" not in mode:
            raise ValueError("text mode unsuported")
        loid = int(pathlib.Path(name).stem)
        return PostgresqlLargeObjectFile(name, loid, mode, self)

    def _save(self, name, content):
        # loid = int(name)
        with connection.cursor() as cursor:
            cursor.execute("select lo_create(0) as loid")
            row = cursor.fetchone()
            loid = row[0]

            cursor.execute("select lo_open(%s, %s)", [loid, MODE_WRITE])
            row = cursor.fetchone()
            fd = row[0]
            for chunk in content.chunks():
                cursor.execute("select lowrite(%s, %s)", [fd, chunk])
            cursor.execute("select lo_close(%s)", [fd])
        suffixes = pathlib.Path(name).suffixes
        suffixes = "".join(suffixes)
        return f"{loid}{suffixes}"

    def delete(self, name):
        # loid = int(name)
        loid = int(pathlib.Path(name).stem)
        with connection.cursor() as cursor:
            cursor.execute("select lo_unlink(%s)", [int(loid)])

    def exists(self, name):
        # loid = int(name)
        try:
            loid = int(pathlib.Path(name).stem)
        except (ValueError, TypeError):
            return False
        with connection.cursor() as cursor:
            cursor.execute("select count(loid) from pg_largeobject where loid=%s limit 1", [loid])
            row = cursor.fetchone()
            return row[0] >= 1

    def listdir(self, path):
        raise NotImplementedError(
            "subclasses of Storage must provide a listdir() method"
        )

    def size(self, name):
        # loid = int(name)
        loid = int(pathlib.Path(name).stem)
        with connection.cursor() as cursor:
            cursor.execute("select lo_open(%s, %s)", [loid, MODE_READ])
            fd = cursor.fetchone()[0]

            cursor.execute("select lo_lseek64(%s, 0, %s)", [fd, SEEK_END])
            cursor.execute("select lo_tell64(%s)", [fd])
            size = cursor.fetchone()[0]

            cursor.execute("select lo_close(%s)", [fd])

            return size

    def url(self, name):
        if self._base_url is None:
            raise ValueError("This file is not accessible via a URL.")
        return urljoin(self._base_url, name).replace("\\", "/")


class PostgresqlLargeObjectFile:
    def __init__(self, name: str, loid: int, mode: str, storage: PostgresqlLargeObjectStorage) -> None:
        self._name = name
        self._loid = loid
        self._storage = storage
        self._file = None
        # self._cursor = connection.cursor()

    def _get_file(self):
        if self._file is None:
            self._file = SpooledTemporaryFile()
            with connection.cursor() as cursor:
                cursor.execute("select lo_get(%s)", [self._loid])
                row = cursor.fetchone()
                data = row[0]
                self._file.write(data)
            self._file.seek(0)
        return self._file

    def _set_file(self, value):
        self._file = value

    file = property(_get_file, _set_file)

    @property
    def size(self):
        if not hasattr(self, "_size"):
            self._size = self._storage.size(self.name)
        return self._size

    def __iter__(self):
        return self.file.__iter__()

    # def readlines(self):
    #     if not self._is_read:
    #         self._storage._start_connection()
    #         self.file = self._storage._read(self.name)
    #         self._is_read = True
    #     return self.file.readlines()

    # def read(self, num_bytes=None):
    #     if not self._is_read:
    #         self._storage._start_connection()
    #         self.file = self._storage._read(self.name)
    #         self._is_read = True
    #     return self.file.read(num_bytes)

    # def write(self, content):
    #     if "w" not in self._mode:
    #         raise AttributeError("File was opened for read-only access.")
    #     self.file = io.BytesIO(content)
    #     self._is_dirty = True
    #     self._is_read = True

    def close(self):
        # if self._is_dirty:
        #     self._storage._start_connection()
        #     self._storage._put_file(self.name, self)
        #     self._storage.disconnect()
        if self._file:
            self._file.close()
        # if self._cursor:
        #     self._cursor.close()
