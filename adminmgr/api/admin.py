from django.contrib import admin
from django.contrib.auth.models import Group, User
from .models import SubmissionAssignmentOne, Team, SubmissionAssignmentTwo, SubmissionAssignmentThree, SubmissionMtech, FinalProject

# admin.site.unregister(User)
# admin.site.unregister(Group)
admin.site.site_header = 'Big Data Assignment'


class AssignmentAdmin(admin.ModelAdmin):
    list_display = ('id', 'team', 'submitted_on')
    list_filter = ('team', 'submitted_on')


admin.site.register(SubmissionAssignmentTwo, AssignmentAdmin)
admin.site.register(Team)
admin.site.register(SubmissionMtech)
admin.site.register(SubmissionAssignmentOne, AssignmentAdmin)
admin.site.register(SubmissionAssignmentThree, AssignmentAdmin)
admin.site.register(FinalProject, AssignmentAdmin)
