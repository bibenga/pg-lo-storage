from django.contrib import admin
from django.urls import path
from django.views.decorators.cache import cache_page

from warehouse.views import large_object_serve

urlpatterns = [
    path("admin/", admin.site.urls),
    path("media/<str:filename>", large_object_serve),
    # the cache does not worke when used streaming response
    path("cached-media/<str:filename>", cache_page(60)(large_object_serve)),
]
