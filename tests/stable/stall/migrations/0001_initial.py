# Generated by Django 5.0.6 on 2024-07-03 16:57

import pg_lo_storage.fields
import pg_lo_storage.storage
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="UserFile",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "file0",
                    pg_lo_storage.fields.DbFileField(
                        blank=True,
                        null=True,
                        storage=pg_lo_storage.storage.DbFileStorage(),
                        upload_to="",
                    ),
                ),
                (
                    "file1",
                    models.FileField(
                        blank=True,
                        null=True,
                        storage=pg_lo_storage.storage.DbFileStorage(),
                        upload_to="",
                    ),
                ),
                ("file2", models.FileField(blank=True, null=True, upload_to="")),
            ],
        ),
    ]