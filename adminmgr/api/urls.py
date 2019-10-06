from django.urls import path
from . import views

app_name = 'api'
urlpatterns = [
    path('', views.Index.as_view()),
    path('assignment1', views.CodeUploadAssignmentOne.as_view()),
    path('assignment2', views.CodeUploadAssignmentTwo.as_view())
]
