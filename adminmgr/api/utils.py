import sys

from celery import shared_task

sys.path.append('../')

from notifymgr.mail import send_mail
from .models import SubmissionAssignmentOne


@shared_task
def run(submission_id):
    sub = SubmissionAssignmentOne.objects.get(id=submission_id)
    # TODO: Run code on hadoop through celery
    # TODO: Run files one after the other.
    team = sub.team
    emails = [team.member_1, team.member_2, team.member_3, team.member_4]
    for email in emails:
        if email != '':
            send_mail(email)
