## Use PostgreSQL large objects for file storage

This storage is created to store **important files** that belong to a user.
**It should not be used to store publicly available files or videos.**
The main benefit is that it works with **transaction** and you can create or delete a file and be confident that the file will still exist if the transaction is rolled back.

When the storage saves a file, a new filename with the template <loid>.<original_extension> is always created and the attribute upload_to is not used.

You can use DbFileIO without a storage.

The settings:
* PG_LO_STORAGE_DB_FOR_READ - a database for read files (default is `default`)
* PG_LO_STORAGE_DB_FOR_WRITE - a database for create and write files (default is `default`)

### Example

Add model:
```python
from django.db import models
from django.db.models.signals import post_delete, post_init, post_save
from django.dispatch import receiver
from pg_lo_storage.fields import DbFileField
from pg_lo_storage.storage import db_file_storage, DbFileStorage

class Invoice(models.Model):
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    # data = DbFileField(storage=DbFileStorage("/invoice"), null=True, blank=True)
    data = models.FileField(storage=DbFileStorage("/invoice"), null=True, blank=True)

@receiver(post_init, sender=Invoice)
def user_file_initialized(sender, instance: Invoice, **kwargs):
    instance._lo_prev_state = {
        'data': instance.data.name if instance.data else None,
    }

@receiver(post_save, sender=Invoice)
def user_file_saved(sender, instance: Invoice, created: bool, **kwargs):
    # this is safe because large objects support transactions
    if not created and hasattr(instance, '_lo_prev_state'):
        state = instance._lo_prev_state
        if state.get('data'):
            instance.data.storage.delete(state['data'])

@receiver(post_delete, sender=Invoice)
def user_file_deleted(sender, instance: Invoice, **kwargs):
    # this is safe because large objects support transactions
    if instance.data:
        instance.data.storage.delete(instance.data.name)
```

Add function to serve files:
```python
from django.http import HttpResponseForbidden
from pg_lo_storage.views import db_serve

@login_required
def invlice_serve(request: HttpRequest, filename: str) -> HttpResponse:
    if not Invoice.objects.filter(user=request.user, data=filename).exists():
        return HttpResponseForbidden()
    return db_serve(request, filename)
```

Work as a file:
```python
from django.db import transaction
from pg_lo_storage.storage import DbFileIO

@transaction.atomic
def write(loid: int, data: bytes, pos: int) -> int:
    # DbFileIO work only with transactions
    with DbFileIO(loid, "wb") as file:
        file.seek(pos)
        file.write(data)
        return file.loid
```
