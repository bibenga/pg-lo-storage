import io
import os
import pathlib
import warnings
from types import TracebackType
from typing import Iterable, Iterator, Self
from urllib.parse import urljoin

from django.conf import settings
from django.core.files.storage import Storage
from django.core.files.storage.mixins import StorageSettingsMixin
from django.core.signals import setting_changed
from django.db import ProgrammingError, connection, transaction
from django.utils.deconstruct import deconstructible
from django.utils.functional import LazyObject

MODE_WRITE = 0x20000

MODE_READ = 0x40000
MODE_READWRITE = MODE_READ | MODE_WRITE

SEEK_SET = io.SEEK_SET  # 0
SEEK_CUR = io.SEEK_CUR  # 1
SEEK_END = io.SEEK_END  # 2

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

    def _get_loid(self, name):
        loid = int(pathlib.Path(name).stem)
        return loid

    def _open(self, name, mode="rb"):
        if "b" not in mode:
            raise ValueError("the text mode is unsuported")
        loid = self._get_loid(name)
        # return PostgresqlLargeObjectFile(name, loid, mode, self)
        f = PostgresqlLargeObjectFile(self, loid, mode)
        f.open()
        if "t" in mode:
            f = io.TextIOWrapper(f)
        return f

    def _save(self, name, content):
        with PostgresqlLargeObjectFile(self, 0, "wb") as f:
            for chunk in content.chunks():
                f.write(chunk)
            loid = f.loid
        suffixes = pathlib.Path(name).suffixes
        suffixes = "".join(suffixes)
        return f"{loid}{suffixes}"

    def delete(self, name):
        # loid = int(name)
        loid = self._get_loid(name)
        try:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute("select lo_unlink(%s)", [int(loid)])
        except ProgrammingError as err:
            # django.db.utils.ProgrammingError: large object 38450 does not exist
            pass

    def exists(self, name):
        with transaction.atomic():
            with self._open(name):
                return True
        return False

    def listdir(self, path):
        raise NotImplementedError()

    def size(self, name):
        with self._open(name) as f:
            return f.size

    def url(self, name):
        if self._base_url is None:
            raise ValueError("This file is not accessible via a URL.")
        return urljoin(self._base_url, name).replace("\\", "/")


class PostgresqlLargeObjectFile(io.IOBase):
    # https://docs.python.org/3/library/io.html#class-hierarchy
    CHUNK_SIZE = 65536

    def __init__(self, storage: PostgresqlLargeObjectStorage, loid: int, mode: str = "rb", name: str = "") -> None:
        self._storage = storage
        self._loid = loid
        self._mode = mode

        if "b" not in mode:
            raise ValueError("the text mode is unsuported")
        if "r" in mode and "w" in mode:
            mode = MODE_READWRITE
        elif "r" in mode:
            mode = MODE_READ
        elif "w" in mode:
            mode = MODE_WRITE
        else:
            raise ValueError(f"the mode '{mode}' is invalid")

        self._cursor = connection.cursor()
        if self._loid == 0:
            self._cursor.execute("select lo_create(0) as loid")
            self._loid = self._cursor.fetchone()[0]
        self._cursor.execute("select lo_open(%s, %s)", [self._loid, mode])
        self._fd = self._cursor.fetchone()[0]
        self._name = name or str(self._loid)

    def __str__(self) -> str:
        return f"<PostgresqlLargeObjectFile: {self._loid}>"

    @property
    def loid(self) -> int:
        return self._loid

    @property
    def size(self) -> int:
        pos = self.tell()
        self.seek(0, os.SEEK_END)
        size = self.tell()
        self.seek(pos, os.SEEK_SET)
        return size

    # Context management protocol
    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None:
        self.close()

    # file protocol
    def open(self, mode=None, *args, **kwargs) -> Self:
        if not self.closed:
            self.close()

        if "r" in mode and "w" in mode:
            mode = MODE_READWRITE
        elif "r" in mode:
            mode = MODE_READ
        elif "w" in mode:
            mode = MODE_WRITE
        else:
            raise ValueError(f"the mode '{mode}' is invalid")
        self._cursor.execute("select lo_open(%s, %s)", [self._loid, mode])
        self._fd = self._cursor.fetchone()[0]
        return self

    def close(self) -> None:
        if not self.closed:
            cursor, self._cursor = self._cursor, None
            fd, self._fd = self._fd, None
            cursor.execute("select lo_close(%s)", [fd])
            cursor.close()

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        data = self.read(self.CHUNK_SIZE)
        if not data:
            raise StopIteration
        return data

    def __del__(self) -> None:
        if not self.closed:
            warnings.warn(
                "Unclosed file {!r}".format(self),
                ResourceWarning,
                stacklevel=2,
                source=self
            )
            self.close()

    @property
    def closed(self) -> bool:
        return self._cursor is None

    def fileno(self):
        return self._fd

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return False

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def name(self) -> str:
        return self._name

    def readable(self) -> bool:
        return "r" in self._mode

    def read(self, size=-1) -> bytes | None:
        if size is None or size < 0:
            return self.readall()
        self._cursor.execute("select loread(%s, %s)", [self._fd, size])
        data = self._cursor.fetchone()[0]
        if not data:
            return None
        return data

    def readall(self) -> bytes | None:
        data = b""
        while True:
            chunk = self.read(self.CHUNK_SIZE)
            if not chunk:
                break
            data += chunk
            if len(chunk) < self.CHUNK_SIZE:
                break
        return data

    def readinto(self, b) -> None:
        while True:
            chunk = self.read(self.CHUNK_SIZE)
            if not chunk:
                break
            b.write(chunk)
            if len(chunk) < self.CHUNK_SIZE:
                break

    def readline(self, size=-1) -> bytes | None:
        pos = self.tell()
        line = b""
        while True:
            chunk = self.read(64)
            if not chunk:
                break
            try:
                i = chunk.index(b"\n")
                line += chunk[0: i + 1]
                break
            except ValueError:
                line += chunk
                if len(line) >= size:
                    break
        if not line:
            return None
        if size > 0 and len(line) > size:
            line = line[0:size]
        self.seek(pos + len(line), os.SEEK_SET)
        return line

    def readlines(self, hint=-1):
        lines = []
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
            if hint is not None and hint > 0 and len(lines) == hint:
                break
        return lines

    def seekable(self):
        return True

    def seek(self, offset, whence=os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            whence = SEEK_SET
        elif whence == os.SEEK_CUR:
            whence = SEEK_CUR
        elif whence == os.SEEK_END:
            whence = SEEK_END
        else:
            raise ValueError("the whence is invalid")
        self._cursor.execute("select lo_lseek64(%s, %s, %s)", [self._fd, offset, whence])
        return self.tell()

    def tell(self):
        self._cursor.execute("select lo_tell64(%s)", [self._fd])
        pos = self._cursor.fetchone()[0]
        return pos

    def truncate(self, size=None):
        if size is None:
            size = self.tell()
        self._cursor.execute("select lo_truncate64(%s, %s)", [self._fd, size])
        return size

    def writable(self) -> bool:
        return "w" in self._mode

    def write(self, b) -> int | None:
        self._cursor.execute("select lowrite(%s, %s)", [self._fd, b])
        return len(b)

    def writelines(self, iterable: Iterable) -> None:
        for s in iterable:
            self.write(s)
