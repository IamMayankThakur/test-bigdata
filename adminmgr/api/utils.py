import sys

sys.path.append('../')
from notifymgr.mail import send_mail
from .models import Submission


def run(submission_id):
    sub = Submission.objects.get(id=submission_id)
    # TODO: Run code o hadoop through celery
    team = sub.team
    emails = [team.member_1, team.member_2, team.member_3, team.member_4]
    for email in emails:
        if email != '':
            send_mail(email)
