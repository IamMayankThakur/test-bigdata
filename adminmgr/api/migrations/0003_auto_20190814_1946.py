# Generated by Django 2.2.4 on 2019-08-14 14:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0002_submission_timestamp'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='submission',
            name='timestamp',
        ),
        migrations.AddField(
            model_name='submission',
            name='submitted_on',
            field=models.DateTimeField(auto_now=True),
        ),
    ]
