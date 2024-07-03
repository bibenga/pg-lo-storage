from django.db import models
from django.db.models.signals import post_delete, post_init, post_save
from django.dispatch import receiver

from warehouse.fields import PostgresqlLargeObjectFileField
from warehouse.storage import postgresql_large_object_storage


class UserFile(models.Model):
    file0 = PostgresqlLargeObjectFileField(null=True, blank=True)
    file1 = models.FileField(storage=postgresql_large_object_storage, null=True, blank=True)
    file2 = models.FileField(null=True, blank=True)


@receiver(post_delete, sender=UserFile)
def auto_delete_file_on_delete(sender, instance: UserFile, **kwargs):
    # if instance.file0:
    #     instance.file0.storage.delete(instance.file0.name)
    pass
