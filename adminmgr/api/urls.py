from django.urls import path
from . import views

app_name = 'api'
urlpatterns = [
    path('', views.code_upload)
]
