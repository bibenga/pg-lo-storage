import io
import os
import pathlib
from functools import cached_property
from types import TracebackType
from typing import Iterable, Iterator, Self
from urllib.parse import urljoin

from django.conf import settings
from django.core.files import File
from django.core.files.storage import Storage
from django.core.files.storage.mixins import StorageSettingsMixin
from django.core.signals import setting_changed
from django.db import ProgrammingError, connections
from django.utils.deconstruct import deconstructible
from django.utils.functional import LazyObject

MODE_WRITE = 0x20000
MODE_READ = 0x40000
MODE_READWRITE = MODE_READ | MODE_WRITE

SEEK_SET = io.SEEK_SET  # 0
SEEK_CUR = io.SEEK_CUR  # 1
SEEK_END = io.SEEK_END  # 2


class DefaultDbFileStorage(LazyObject):
    def _setup(self):
        from warehouse.storage import DbFileStorage
        self._wrapped = DbFileStorage()


db_file_storage: 'DbFileStorage' = DefaultDbFileStorage()


def db_for_read():
    return getattr(settings, "WAREHOUSE_DB_FOR_READ", "default")


def db_for_write():
    return getattr(settings, "WAREHOUSE_DB_FOR_WRITE", "default")


@deconstructible(path="warehouse.storage.DbFileStorage")
class DbFileStorage(Storage, StorageSettingsMixin):
    def __init__(self, base_url: str | None = None) -> None:
        self._base_url = base_url
        setting_changed.connect(self._clear_cached_properties)

    @cached_property
    def base_url(self):
        if self._base_url is not None and not self._base_url.endswith("/"):
            self._base_url += "/"
        return self._value_or_setting(self._base_url, settings.MEDIA_URL)

    def generate_filename(self, filename: str) -> str:
        """The operation is not supported and real name was provided on the save operation"""
        return filename

    def get_available_name(self, filename: str, max_length=None) -> str:
        """The operation is not supported and real name was provided on the save operation"""
        return filename

    def is_valid_name(self, name: str) -> bool:
        try:
            loid = self._get_loid(name)
            return loid != 0
        except ValueError:
            return False

    def _get_loid(self, name) -> int:
        try:
            return int(pathlib.Path(name).stem)
        except (ValueError, TypeError) as e:
            raise ValueError(f"the name '{name}' is invalid")

    def _open(self, name, mode="rb") -> File:
        if "b" not in mode:
            raise ValueError("the text mode is unsuported")
        if not self.exists(name):
            raise FileNotFoundError("File does not exist: %s" % name)
        loid = self._get_loid(name)
        file = DbFileIO(loid, mode, name)
        return DbFile(file, file.name)

    def _save(self, name: str, content: Iterable[bytes]) -> str:
        with DbFileIO(0, "wb", name) as f:
            for chunk in content.chunks():
                f.write(chunk)
            return f.name

    def delete(self, name: str) -> None:
        loid = self._get_loid(name)
        try:
            with connections[db_for_write()].cursor() as cursor:
                cursor.execute("select lo_unlink(%s)", [int(loid)])
        except ProgrammingError as err:
            if str(err) == f"large object {loid} does not exist":
                return
            raise

    def exists(self, name: str) -> bool:
        loid = self._get_loid(name)
        with connections[db_for_read()].cursor() as cursor:
            cursor.execute("select exists(select loid from pg_largeobject where loid=%s)", [loid])
            return cursor.fetchone()[0]

    def listdir(self, path: str):
        raise PermissionError()

    def size(self, name: str) -> int:
        with self._open(name) as f:
            return f.size

    def url(self, name: str) -> str:
        self._get_loid(name)
        if self.base_url is None:
            raise ValueError("This file is not accessible via a URL.")
        return urljoin(self.base_url, name).replace("\\", "/")


class DbFile(File):
    def open(self, mode: str | None = ...) -> Self:
        self.file.open(mode)
        return self


class DbFileIO(io.IOBase):
    # https://docs.python.org/3/library/io.html#class-hierarchy
    CHUNK_SIZE = 65536
    LINE_SIZE = 64

    def __init__(self, loid: int, mode: str = "rb", name: str = "") -> None:
        self._loid = loid
        self._mode = mode
        self._name = name

        if "t" in mode:
            raise ValueError("the text mode is unsuported")
        if "r" in mode and "w" in mode:
            mode = MODE_READWRITE
            self._using = db_for_write()
        elif "r" in mode:
            mode = MODE_READ
            self._using = db_for_read()
        elif "w" in mode:
            mode = MODE_WRITE
            self._using = db_for_write()
        else:
            raise ValueError(f"the mode '{mode}' is invalid")

        with connections[self._using].cursor() as cursor:
            if self._loid == 0:
                cursor.execute("select lo_create(0) as loid")
                self._loid = cursor.fetchone()[0]
                self._name = str(self._loid) + "".join(pathlib.Path(name).suffixes)
            cursor.execute("select lo_open(%s, %s)", [self._loid, mode])
            self._fd = cursor.fetchone()[0]
            # self._name = name or str(self._loid)

    def __str__(self) -> str:
        return self._name or str(self._loid)

    def __repr__(self) -> str:
        return f"<PostgresqlLargeObjectFile: {self._loid}, {self._name}>"

    @property
    def loid(self) -> int:
        return self._loid

    @property
    def size(self) -> int:
        with connections[self._using].cursor() as cursor:
            # pos = self.tell()
            cursor.execute("select lo_tell64(%s)", [self._fd])
            pos = cursor.fetchone()[0]

            # self.seek(0, os.SEEK_END)
            cursor.execute("select lo_lseek64(%s, %s, %s)", [self._fd, 0, os.SEEK_END])

            # size = self.tell()
            cursor.execute("select lo_tell64(%s)", [self._fd])
            size = cursor.fetchone()[0]

            # self.seek(pos, os.SEEK_SET)
            cursor.execute("select lo_lseek64(%s, %s, %s)", [self._fd, pos, os.SEEK_SET])
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

        if "t" in mode:
            raise ValueError("the text mode is unsuported")
        if "r" in mode and "w" in mode:
            mode = MODE_READWRITE
            self._using = db_for_write()
        elif "r" in mode:
            mode = MODE_READ
            self._using = db_for_read()
        elif "w" in mode:
            mode = MODE_WRITE
            self._using = db_for_write()
        else:
            raise ValueError(f"the mode '{mode}' is invalid")

        with connections[self._using].cursor() as cursor:
            cursor.execute("select lo_open(%s, %s)", [self._loid, mode])
            self._fd = cursor.fetchone()[0]
        return self

    def close(self) -> None:
        if not self.closed:
            fd, self._fd = self._fd, None
            with connections[self._using].cursor() as cursor:
                cursor.execute("select lo_close(%s)", [fd])

    def __iter__(self) -> Iterator[bytes]:
        pos = self.tell()
        buf = None
        chunk_len = 0
        while True:
            if chunk_len > 0:
                pos += chunk_len
                self.seek(pos)
            chunk = self.read(self.CHUNK_SIZE)
            if not chunk:
                break
            chunk_len = len(chunk)
            if buf:
                buf += chunk
            else:
                buf = chunk

            s = 0
            while True:
                try:
                    i = buf.index(b"\n", s)
                    d = buf[s: i + 1]
                    self.seek(len(d), os.SEEK_CUR)
                    yield d
                    s = i + 1
                    if s >= len(buf):
                        buf = None
                        break
                except ValueError:
                    if s > 0:
                        buf = buf[s:]
                    break
        if buf:
            self.seek(len(buf), os.SEEK_CUR)
            yield buf

    # doesn't work
    # def __del__(self) -> None:
    #     if not self.closed:
    #         warnings.warn(
    #             "Unclosed file {!r}".format(self),
    #             ResourceWarning,
    #             stacklevel=2,
    #             source=self
    #         )
    #         self.close()

    @property
    def closed(self) -> bool:
        return self._fd is None

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
    def name(self) -> str | None:
        return self._name

    def readable(self) -> bool:
        # return "r" in self._mode
        return True

    def read(self, size: int = -1) -> bytes | None:
        if size is None or size < 0:
            return self.readall()
        with connections[self._using].cursor() as cursor:
            cursor.execute("select loread(%s, %s)", [self._fd, size])
            data = cursor.fetchone()[0]
        if not data:
            return None
        return data

    def read1(self, size: int = -1) -> bytes | None:
        return self.read(size)

    def readall(self) -> bytes | None:
        data = b""
        while True:
            chunk = self.read(self.CHUNK_SIZE)
            if not chunk:
                break
            data += chunk
        return data

    def readinto(self, b) -> None:
        while True:
            chunk = self.read(self.CHUNK_SIZE)
            if not chunk:
                break
            b.write(chunk)

    def readinto1(self, b) -> None:
        self.readinto(b)

    def readline(self, size: int = -1) -> bytes | None:
        if size == 0:
            return b""
        pos = self.tell()
        line = b""
        while True:
            chunk = self.read(self.LINE_SIZE)
            if not chunk:
                break
            try:
                i = chunk.index(b"\n")
                line += chunk[0: i + 1]
                break
            except ValueError:
                # b"\n" is not found
                line += chunk
                if size > 0 and len(line) >= size:
                    break
        if not line:
            return None
        if size > 0 and len(line) > size:
            line = line[0:size]
        self.seek(pos + len(line), os.SEEK_SET)
        return line

    def readlines(self, hint: int = -1) -> list[bytes]:
        if hint == 0:
            return []
        lines = []
        for line in self:
            lines.append(line)
            if hint is not None and hint > 0 and len(lines) == hint:
                break
        return lines

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence=os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            whence = SEEK_SET
        elif whence == os.SEEK_CUR:
            whence = SEEK_CUR
        elif whence == os.SEEK_END:
            whence = SEEK_END
        else:
            raise ValueError("the whence is invalid")
        with connections[self._using].cursor() as cursor:
            cursor.execute("select lo_lseek64(%s, %s, %s)", [self._fd, offset, whence])
        return self.tell()

    def tell(self) -> int:
        with connections[self._using].cursor() as cursor:
            cursor.execute("select lo_tell64(%s)", [self._fd])
            pos = cursor.fetchone()[0]
        return pos

    def truncate(self, size: int | None = None) -> int:
        if size is None:
            size = self.tell()
        with connections[self._using].cursor() as cursor:
            cursor.execute("select lo_truncate64(%s, %s)", [self._fd, size])
        return size

    def writable(self) -> bool:
        return "w" in self._mode

    def write(self, b: bytes) -> int | None:
        with connections[self._using].cursor() as cursor:
            cursor.execute("select lowrite(%s, %s)", [self._fd, b])
        return len(b)

    def writelines(self, iterable: Iterable[bytes]) -> None:
        for s in iterable:
            self.write(s)
