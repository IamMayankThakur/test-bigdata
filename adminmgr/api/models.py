from django.db import models
from django.core.files.storage import FileSystemStorage
import os


class OverwriteStorage(FileSystemStorage):

    def get_available_name(self, name, max_length=None):
        if self.exists(name):
            os.remove(os.path.join(self.location, name))
        return name


class Team(models.Model):
    team_name = models.CharField(max_length=128, unique=True, blank=False)
    member_1 = models.CharField(max_length=128, unique=True, blank=False)
    member_2 = models.CharField(max_length=128, unique=True, blank=True)
    member_3 = models.CharField(max_length=128, unique=False, blank=True)
    member_4 = models.CharField(max_length=128, unique=False, blank=True)

    def __str__(self):
        return self.team_name


class SubmissionAssignmentOne(models.Model):
    team = models.ForeignKey(Team, on_delete=models.CASCADE)
    submitted_on = models.DateTimeField(auto_now=True)
    python = models.BooleanField(default=False)
    java = models.BooleanField(default=True)
    code_config = models.FileField(blank=False, upload_to='code/config', storage=OverwriteStorage())
    code_file_java_task_1 = models.FileField(blank=True, null=True, upload_to='code/java/1', storage=OverwriteStorage())
    code_file_java_task_2 = models.FileField(blank=True, null=True, upload_to='code/java/2', storage=OverwriteStorage())
    code_file_java_task_3 = models.FileField(blank=True, null=True, upload_to='code/java/3', storage=OverwriteStorage())
    code_file_python_map_1 = models.FileField(blank=True, null=True, upload_to='code/python/map1',
                                              storage=OverwriteStorage())
    code_file_python_map_2 = models.FileField(blank=True, null=True, upload_to='code/python/map2',
                                              storage=OverwriteStorage())
    code_file_python_map_3 = models.FileField(blank=True, null=True, upload_to='code/python/map3',
                                              storage=OverwriteStorage())
    code_file_python_reduce_1 = models.FileField(blank=True, null=True, upload_to='code/python/red1',
                                                 storage=OverwriteStorage())
    code_file_python_reduce_2 = models.FileField(blank=True, null=True, upload_to='code/python/red2',
                                                 storage=OverwriteStorage())
    code_file_python_reduce_3 = models.FileField(blank=True, null=True, upload_to='code/python/red3',
                                                 storage=OverwriteStorage())
    score_1 = models.IntegerField(default=-1)
    score_2 = models.IntegerField(default=-1)
    score_3 = models.IntegerField(default=-1)
    score_4 = models.IntegerField(default=-1)
    remarks = models.TextField(null=True, blank=True, default="None")
    output = models.TextField(null=True, blank=True, default="None")

    def __str__(self):
        return str(self.id)


class SubmissionAssignmentTwo(models.Model):
    team = models.ForeignKey(Team, on_delete=models.CASCADE)
    submitted_on = models.DateTimeField(auto_now=True)
    code_file_task_1 = models.FileField(blank=False, upload_to='code/A2/python/task1')
    # code_file_task_2 = models.FileField(blank=False, upload_to='code/A2/python/task2')
    score_1 = models.IntegerField(default=-1)
    score_2 = models.IntegerField(default=-1)
    # score_3 = models.IntegerField(default=-1)
    # score_4 = models.IntegerField(default=-1)
    remarks = models.TextField(null=True, blank=True, default="None")

    def __str__(self):
        return str(self.id)