from django.contrib import admin
from .models import Submission, Team
# Register your models here.

admin.site.register([Team, Submission])
