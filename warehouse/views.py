import mimetypes

from django.http import FileResponse, HttpRequest, HttpResponse

from warehouse.storage import postgresql_large_object_storage


def lo_file(request: HttpRequest, filename: str) -> HttpResponse:
    content_type, encoding = mimetypes.guess_type(filename)
    content_type = content_type or "application/octet-stream"
    f = postgresql_large_object_storage.open(filename)
    response = FileResponse(f, content_type=content_type)
    if encoding:
        response.headers["Content-Encoding"] = encoding
    return response
