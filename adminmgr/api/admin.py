from django.contrib import admin
from django.contrib.auth.models import Group, User
from .models import SubmissionAssignmentOne, Team, SubmissionAssignmentTwo
# Register your models here.

admin.site.site_header = 'Big Data Assignment'

class AssignmentAdmin(admin.ModelAdmin):
    list_display = ('id', 'team', 'submitted_on')
    list_filter = ('id', 'submitted_on')


admin.site.register(SubmissionAssignmentTwo, AssignmentAdmin)
admin.site.register(Team)
admin.site.register(SubmissionAssignmentOne, AssignmentAdmin)
# admin.site.unregister([Group, User])