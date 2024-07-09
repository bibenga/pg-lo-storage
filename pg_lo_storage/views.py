import io
import mimetypes
from tempfile import SpooledTemporaryFile

from django.core.files.storage import Storage
from django.db import transaction
from django.http import FileResponse, HttpRequest, HttpResponse, HttpResponseNotFound

from pg_lo_storage.storage import DbFileIO, db_file_storage

default_content_type = "application/octet-stream"


def db_serve(request: HttpRequest, filename: str) -> HttpResponse:
    storage = db_file_storage
    if not storage.is_valid_name(filename):
        return HttpResponseNotFound()
    if not storage.exists(filename):
        return HttpResponseNotFound()

    content_type, encoding = mimetypes.guess_type(filename)
    content_type = content_type or default_content_type

    range_header = request.headers.get("Range")
    if range_header:
        with transaction.atomic():
            size = storage.size(filename)
        start, end = get_byte_range(range_header, size)
        if start >= end:
            return HttpResponse(status=416, headers={"Content-Range": f"bytes */{size}"})

        with transaction.atomic():
            file = get_partial_file(storage, filename, start, end)
        response = FileResponse(file, content_type=content_type, filename=filename,
                                status=206, headers={"Content-Range": f"bytes {start}-{end}/{size}"})
        if encoding:
            response.headers["Content-Encoding"] = encoding
        return response

    else:
        with transaction.atomic():
            file = get_file(storage, filename)
        response = FileResponse(file, content_type=content_type, filename=filename)
        if encoding:
            response.headers["Content-Encoding"] = encoding
        return response


def get_file(storage: Storage, filename: str) -> io.IOBase:
    t = SpooledTemporaryFile()
    try:
        with storage.open(filename) as f:
            for chunk in f:
                t.write(chunk)
    except:
        t.close()
        raise
    t.seek(0)
    return t


def get_partial_file(storage: Storage, filename: str, start: int, end: int) -> io.IOBase:
    t = SpooledTemporaryFile()
    try:
        with storage.open(filename) as f:
            f.seek(start)
            remaining = end - start + 1
            while remaining > 0:
                chunk_size = min(remaining, DbFileIO.CHUNK_SIZE)
                chunk = f.read(chunk_size)
                t.write(chunk)
                remaining -= len(chunk)
    except:
        t.close()
        raise
    t.seek(0)
    return t


def get_byte_range(range_header, size):
    start, end = parse_byte_range(range_header)
    if start < 0:
        start = max(start + size, 0)
    if end is None:
        end = size - 1
    else:
        end = min(end, size - 1)
    return start, end


def parse_byte_range(range_header):
    units, _, range_spec = range_header.strip().partition("=")
    if units != "bytes":
        raise ValueError()
    # Only handle a single range spec. Multiple ranges will trigger a
    # ValueError below which will result in the Range header being ignored
    start_str, sep, end_str = range_spec.strip().partition("-")
    if not sep:
        raise ValueError()
    if not start_str:
        start = -int(end_str)
        end = None
    else:
        start = int(start_str)
        end = int(end_str) if end_str else None
    return start, end
