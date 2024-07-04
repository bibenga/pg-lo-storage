from django.db import models
from django.db.models.fields.files import FileDescriptor

from .storage import postgresql_large_object_storage


class PostgresqlLargeObjectFileDescriptor(FileDescriptor):
    # see ImageFileDescriptor
    def __set__(self, instance, value):
        prev_key = f"_lo_{self.field.attname}_previous"
        if prev_key not in instance.__dict__:
            instance.__dict__[prev_key] = value
        super().__set__(instance, value)
        # if previous_file is not None:
        #     self.field.update_dimension_fields(instance, force=True)


class PostgresqlLargeObjectFileField(models.FileField):
    descriptor_class = PostgresqlLargeObjectFileDescriptor

    def __init__(self, verbose_name=None, name=None, storage=None, **kwargs) -> None:
        super().__init__(
            verbose_name=verbose_name,
            name=name,
            storage=storage or postgresql_large_object_storage,
            **kwargs
        )
