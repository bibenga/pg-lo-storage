from django.db import models

from .storage import db_file_storage


class DbFileField(models.FileField):
    def __init__(
        self, verbose_name=None, name=None, upload_to="", storage=None, **kwargs
    ):
        super().__init__(
            verbose_name=verbose_name,
            name=name,
            upload_to=upload_to,
            storage=storage or db_file_storage,
            **kwargs
        )


class DbImageField(models.ImageField):
    # descriptor_class = PostgresqlLargeObjectFileDescriptor

    def __init__(
        self,
        verbose_name=None,
        name=None,
        width_field=None,
        height_field=None,
        storage=None,
        **kwargs,
    ):
        super().__init__(
            verbose_name=verbose_name,
            name=name,
            width_field=width_field,
            height_field=height_field,
            storage=storage or db_file_storage,
            **kwargs
        )
