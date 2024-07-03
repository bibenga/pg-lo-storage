from django.contrib import admin
from .models import UserFile


@admin.register(UserFile)
class UserFileAdmin(admin.ModelAdmin):
    list_display = ("id", )
