import mimetypes
from tempfile import SpooledTemporaryFile

from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import FileResponse, HttpRequest, HttpResponse

from warehouse.storage import postgresql_large_object_storage


@login_required
def large_object_serve(request: HttpRequest, filename: str) -> HttpResponse:
    content_type, encoding = mimetypes.guess_type(filename)
    content_type = content_type or "application/octet-stream"
    t = SpooledTemporaryFile()
    try:
        with transaction.atomic():
            with postgresql_large_object_storage.open(filename) as f:
                for chunk in f:
                    t.write(chunk)
    except:
        t.close()
        raise
    t.seek(0)
    response = FileResponse(t, content_type=content_type, filename=filename)
    if encoding:
        response.headers["Content-Encoding"] = encoding
    return response
