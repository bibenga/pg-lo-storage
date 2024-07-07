from django.contrib import admin
from django.urls import path
from django.views.decorators.cache import cache_page

from warehouse.views import db_serve

urlpatterns = [
    path("admin/", admin.site.urls),
    path("media/<str:filename>", db_serve),
    # the cache does not work when used streaming response
    path("cached-media/<str:filename>", cache_page(60)(db_serve)),
]
