# warehouse-py

Use PostgreSQL large objects for file storage. The main benefit is that it works with **transaction**.
The DbFileStorage use a template `<loid>.<original_extension>` for field (where loid is a large object id).
The DbFileIO is a file-like object that translates calls to SQL.

Some links:
* https://docs.djangoproject.com/en/5.0/ref/files/storage/
* https://docs.python.org/3/library/io.html
* https://www.postgresql.org/docs/16/lo-funcs.html
* https://www.postgresql.org/docs/current/lo-interfaces.html

```python
from django.db import models
from django.db.models.signals import post_delete, post_init, post_save
from django.dispatch import receiver

from warehouse.fields import DbFileField
from warehouse.storage import db_file_storage


class SomeModel(models.Model):
    data = DbFileField(null=True, blank=True)
    or_just_so = models.FileField(storage=db_file_storage, null=True, blank=True)


@receiver(post_init, sender=SomeModel)
def user_file_initialized(sender, instance: SomeModel, **kwargs):
    instance._lo_prev_state = {
        'data': instance.data.name if instance.data else None,
    }


@receiver(post_save, sender=SomeModel)
def user_file_saved(sender, instance: SomeModel, created: bool, **kwargs):
    # this is safe because large objects support transactions
    if not created and hasattr(instance, '_lo_prev_state'):
        state = instance._lo_prev_state
        if state.get('data'):
            instance.data.storage.delete(state['data'])


@receiver(post_delete, sender=SomeModel)
def user_file_deleted(sender, instance: SomeModel, **kwargs):
    # this is safe because large objects support transactions
    if instance.data:
        instance.data.storage.delete(instance.data.name)
```

For serve files you can use example code:
```python
import mimetypes
from tempfile import SpooledTemporaryFile
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import FileResponse, HttpRequest, HttpResponse, HttpResponseNotFound
from warehouse.storage import db_file_storage

@login_required
def large_object_serve(request: HttpRequest, filename: str) -> HttpResponse:
    content_type, encoding = mimetypes.guess_type(filename)
    content_type = content_type or "application/octet-stream"
    storage = db_file_storage
    if not storage.exists(filename):
        return HttpResponseNotFound()
    t = SpooledTemporaryFile()
    try:
        with transaction.atomic():
            with storage.open(filename) as f:
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

```