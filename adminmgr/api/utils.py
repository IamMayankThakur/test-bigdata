from celery import shared_task

from notifymgr.mail import send_mail
from testmgr.executecode import exe
from .models import SubmissionAssignmentOne, Team
import pandas as pd


@shared_task
def run(submission_id):
    sub = SubmissionAssignmentOne.objects.get(id=submission_id)
    exe(submission_id=submission_id)
    team = sub.team
    emails = [team.member_1, team.member_2, team.member_3, team.member_4]
    for email in emails:
        if email != '':
            send_mail(email)


def port_csv_to_db():
    csv_path = "/home/nishant/Documents/sem7/TA/new_bd_resp - new_bd_resp.csv.csv"
    df = pd.read_csv(csv_path)
    for index, row in df.iterrows():
        try:
            team_obj = Team(
                team_name=row["Enter team name"],
                member_1=row["Team member 1 email"],
                member_2=row["Team member 2 email"],
                member_3=row["Team member 3 email"],
                member_4=row["Team member 4 email"]
            )
            team_obj.save()
        except Exception as e:
            print(e)
            print("Invalid row")
