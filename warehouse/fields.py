from django.db import models

from .storage import postgresql_large_object_storage


class PostgresqlLargeObjectFileField(models.FileField):
    def __init__(self, verbose_name=None, name=None, storage=None, **kwargs) -> None:
        super().__init__(
            verbose_name=verbose_name,
            name=name,
            storage=storage or postgresql_large_object_storage,
            **kwargs
        )
