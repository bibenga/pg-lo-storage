from django.contrib import admin
from django.contrib.auth.decorators import login_required
from django.urls import path
from django.views.decorators.cache import cache_page

from pg_lo_storage.views import db_serve

urlpatterns = [
    path("admin/", admin.site.urls),
    path("media/<str:filename>", login_required(db_serve)),
    # the cache does not work when used streaming response
    path("cached-media/<str:filename>", cache_page(60)(login_required(db_serve))),
]
