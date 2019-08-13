# Generated by Django 2.2.4 on 2019-08-10 10:05

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Team',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('member_1', models.CharField(max_length=128, unique=True)),
                ('member_2', models.CharField(blank=True, max_length=128, unique=True)),
                ('member_3', models.CharField(blank=True, max_length=128, unique=True)),
                ('member_4', models.CharField(blank=True, max_length=128, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='Submission',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('assignment_no', models.IntegerField(default=1)),
                ('code_file', models.FileField(upload_to='code/%Y/%m/%d/')),
                ('test_1', models.IntegerField(default=-1)),
                ('test_2', models.IntegerField(default=-1)),
                ('test_3', models.IntegerField(default=-1)),
                ('test_4', models.IntegerField(default=-1)),
                ('team', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='api.Team')),
            ],
        ),
    ]
