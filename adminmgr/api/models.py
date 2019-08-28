from django.db import models


class Team(models.Model):
    team_name = models.CharField(max_length=128, unique=True, blank=False)
    member_1 = models.CharField(max_length=128, unique=True, blank=False)
    member_2 = models.CharField(max_length=128, unique=True, blank=True)
    member_3 = models.CharField(max_length=128, unique=True, blank=True)
    member_4 = models.CharField(max_length=128, unique=True, blank=True)


class SubmissionAssignmentOne(models.Model):
    team = models.ForeignKey(Team, on_delete=models.CASCADE)
    submitted_on = models.DateTimeField(auto_now=True)
    python = models.BooleanField(default=False)
    java = models.BooleanField(default=True)
    code_config = models.FileField(blank=False, upload_to='code/config')
    code_file_java_task_1 = models.FileField(blank=False, upload_to='code/java/1')
    code_file_java_task_2 = models.FileField(blank=False, upload_to='code/java/2')
    code_file_java_task_3 = models.FileField(blank=False, upload_to='code/java/3')
    code_file_python_map_1 = models.FileField(blank=False, upload_to='code/python/map1')
    code_file_python_map_2 = models.FileField(blank=False, upload_to='code/python/map2')
    code_file_python_map_3 = models.FileField(blank=False, upload_to='code/python/map3')
    code_file_python_reduce_1 = models.FileField(blank=False, upload_to='code/python/red1')
    code_file_python_reduce_2 = models.FileField(blank=False, upload_to='code/python/red2')
    code_file_python_reduce_3 = models.FileField(blank=False, upload_to='code/python/red3')
    score_1 = models.IntegerField(default=-1)
    score_2 = models.IntegerField(default=-1)
    score_3 = models.IntegerField(default=-1)
    score_4 = models.IntegerField(default=-1)
    remarks = models.TextField(null=True, blank=True, default="None")
    output = models.TextField(null=True, blank=True, default="None")
