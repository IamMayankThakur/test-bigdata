from django.db import models


class Team(models.Model):
    team_name = models.CharField(max_length=128, unique=True, blank=False)
    member_1 = models.CharField(max_length=128, unique=True, blank=False)
    member_2 = models.CharField(max_length=128, unique=True, blank=True)
    member_3 = models.CharField(max_length=128, unique=True, blank=True)
    member_4 = models.CharField(max_length=128, unique=True, blank=True)


class Submission(models.Model):
    team = models.ForeignKey(Team, on_delete=models.CASCADE)
    submitted_on = models.DateTimeField(auto_now=True)
    assignment_no = models.IntegerField(default=1, blank=False)
    code_file = models.FileField(blank=False, upload_to='code/%Y/%m/%d/')
    test_1 = models.IntegerField(default=-1)
    test_2 = models.IntegerField(default=-1)
    test_3 = models.IntegerField(default=-1)
    test_4 = models.IntegerField(default=-1)
    remarks = models.TextField(null=True, blank=True, default="None")
