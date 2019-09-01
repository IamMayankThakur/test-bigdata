# Generated by Django 2.2.4 on 2019-09-01 07:47

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0008_team_team_name'),
    ]

    operations = [
        migrations.CreateModel(
            name='SubmissionAssignmentOne',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('submitted_on', models.DateTimeField(auto_now=True)),
                ('python', models.BooleanField(default=False)),
                ('java', models.BooleanField(default=True)),
                ('code_config', models.FileField(upload_to='code/config')),
                ('code_file_java_task_1', models.FileField(blank=True, null=True, upload_to='code/java/1')),
                ('code_file_java_task_2', models.FileField(blank=True, null=True, upload_to='code/java/2')),
                ('code_file_java_task_3', models.FileField(blank=True, null=True, upload_to='code/java/3')),
                ('code_file_python_map_1', models.FileField(blank=True, null=True, upload_to='code/python/map1')),
                ('code_file_python_map_2', models.FileField(blank=True, null=True, upload_to='code/python/map2')),
                ('code_file_python_map_3', models.FileField(blank=True, null=True, upload_to='code/python/map3')),
                ('code_file_python_reduce_1', models.FileField(blank=True, null=True, upload_to='code/python/red1')),
                ('code_file_python_reduce_2', models.FileField(blank=True, null=True, upload_to='code/python/red2')),
                ('code_file_python_reduce_3', models.FileField(blank=True, null=True, upload_to='code/python/red3')),
                ('score_1', models.IntegerField(default=-1)),
                ('score_2', models.IntegerField(default=-1)),
                ('score_3', models.IntegerField(default=-1)),
                ('score_4', models.IntegerField(default=-1)),
                ('remarks', models.TextField(blank=True, default='None', null=True)),
                ('output', models.TextField(blank=True, default='None', null=True)),
                ('team', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='api.Team')),
            ],
        ),
        migrations.DeleteModel(
            name='Submission',
        ),
    ]
