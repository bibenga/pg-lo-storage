# warehouse-py

Use PostgreSQL large objects for file storage.
This storage is created to store **important files** that belong to a user.
**It should not be used to store publicly available files or videos.**
The main benefit is that it works with **transaction** and you can create or delete a file and be confident that the file will still exist if the transaction is rolled back.

When the storage saves a file, a new filename with the template <loid>.<original_extension> is always created and the attribute upload_to is not used.

You can use DbFileIO without a storage.

The settings:
* WAREHOUSE_DB_FOR_READ - a database for read files (default is `default`)
* WAREHOUSE_DB_FOR_WRITE - a database for create and write files (default is `default`)

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
from warehouse.storage import db_file_storage, DbFileStorage


class Invoice(models.Model):
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    data = DbFileField(null=True, blank=True)
    or_just_so = models.FileField(storage=DbFileStorage("/invoice"), null=True, blank=True)


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

For serve files you can use example code:
```python
from django.http import HttpResponseForbidden
from warehouse.views import db_serve

@login_required
def large_object_serve(request: HttpRequest, filename: str) -> HttpResponse:
    if not Invoice.objects.filter(user=request.user, data=filename).exists():
        return HttpResponseForbidden()
    return db_serve(request, filename)
```