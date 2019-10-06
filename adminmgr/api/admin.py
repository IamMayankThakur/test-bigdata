from django.contrib import admin
from .models import SubmissionAssignmentOne, Team, SubmissionAssignmentTwo
# Register your models here.

admin.site.register([Team, SubmissionAssignmentOne, SubmissionAssignmentTwo])
