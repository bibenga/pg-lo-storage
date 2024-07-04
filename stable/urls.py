from django.contrib import admin
from django.urls import path
from django.views.decorators.cache import cache_page

from warehouse.views import lo_file

urlpatterns = [
    path("admin/", admin.site.urls),
    path("media/<str:filename>", lo_file),
    path("cached-media/<str:filename>", cache_page(60)(lo_file)),
]
