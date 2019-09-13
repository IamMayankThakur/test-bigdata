# import pandas as pd
from celery import shared_task

from notifymgr.mail import send_mail
from testmgr.executecode import exe


@shared_task
def run(submission_id):
    exe(submission_id=submission_id)
    send_mail(submission_id)
    print("Submission Evaluation Complete")


# def port_csv_to_db():
#     csv_path = "/home/ubuntu/test-bigdata/data_reg.csv"
#     df = pd.read_csv(csv_path)
#     for index, row in df.iterrows():
#         try:
#             team_obj = Team(
#                 team_name=row["Enter team name"],
#                 member_1=row["Team member 1 email"],
#                 member_2=row["Team member 2 email"],
#                 member_3=row["Team member 3 email"],
#                 member_4=row["Team member 4 email"]
#             )
#             team_obj.save()
#         except Exception as e:
#             print(e)
#             print("Invalid row")


import csv
from api.models import SubmissionAssignmentOne
from django.db.models import Max
import pdb


def find_row(team_name):
    csv_file = csv.reader(open('new_bd_resp.csv', "r"), delimiter=",")
    for row in csv_file:
        if team_name == row[24]:
            return row


def get_max_marks():
    s1 = list(SubmissionAssignmentOne.objects.values(
        'team__team_name').annotate(Max('score_1')))
    s2 = list(SubmissionAssignmentOne.objects.values(
        'team__team_name').annotate(Max('score_2')))
    s3 = list(SubmissionAssignmentOne.objects.values(
        'team__team_name').annotate(Max('score_3')))
    s4 = list(SubmissionAssignmentOne.objects.values(
        'team__team_name').annotate(Max('score_4')))

    allrows = []
    allrows.append(
        ['Team Name', 'SRN1', 'Viva Marks1', 'SRN2', 'Viva Marks2', 'SRN3', 'Viva Marks3', 'SRN4', 'Viva Marks4',
         'Task1 Marks', 'Task2 Marks', 'Task3 Marks', 'Task4 Marks'])
    # for i in range(len(s1)):
    #     row = []
    #     srn_info = find_row(s1[i]['team__team_name']
    #     pdb.set_trace()
    #     print(srn_info)
    srn_info = find_row(s1[0]['team__team_name'])
    print(srn_info)
    print(s1)
