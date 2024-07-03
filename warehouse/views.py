from django.http import FileResponse, HttpRequest, HttpResponse

from warehouse.storage import postgresql_large_object_storage


def lo_file(request: HttpRequest, filename: str) -> HttpResponse:
    f = postgresql_large_object_storage.open(filename)
    content_type = None
    content_type = content_type or "application/octet-stream"
    response = FileResponse(f, content_type=content_type)
    return response
