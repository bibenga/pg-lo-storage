from django.db import models
from django.db.models.signals import post_delete, post_init, post_save
from django.dispatch import receiver

from warehouse.fields import PostgresqlLargeObjectFileField
from warehouse.storage import postgresql_large_object_storage


class UserFile(models.Model):
    file0 = PostgresqlLargeObjectFileField(null=True, blank=True)
    file1 = models.FileField(storage=postgresql_large_object_storage, null=True, blank=True)
    file2 = models.FileField(null=True, blank=True)


@receiver(post_init, sender=UserFile)
def user_file_initialized(sender, instance: UserFile, **kwargs):
    instance._lo_prev_state = {
        'file0': instance.file0.name if instance.file0 else None,
        'file1': instance.file1.name if instance.file1 else None,
    }


@receiver(post_save, sender=UserFile)
def user_file_saved(sender, instance: UserFile, created: bool, **kwargs):
    if not created and hasattr(instance, '_lo_prev_state'):
        state = instance._lo_prev_state
        if state.get('file0'):
            instance.file0.storage.delete(state['file0'])
        if state.get('file1'):
            instance.file1.storage.delete(state['file1'])


@receiver(post_delete, sender=UserFile)
def user_file_deleted(sender, instance: UserFile, **kwargs):
    if instance.file0:
        instance.file0.storage.delete(instance.file0.name)
    if instance.file1:
        instance.file1.storage.delete(instance.file1.name)
