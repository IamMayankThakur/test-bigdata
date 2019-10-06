from django.contrib import admin
from django.contrib.auth.models import Group, User
from .models import SubmissionAssignmentOne, Team, SubmissionAssignmentTwo
# Register your models here.

admin.site.site_header = 'Big Data Assignment'

admin.site.register([Team, SubmissionAssignmentOne, SubmissionAssignmentTwo])
admin.site.unregister([Group, User])