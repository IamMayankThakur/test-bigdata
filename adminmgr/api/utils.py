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


def get_max_marks():
    s1 = list(SubmissionAssignmentOne.objects.values(
        'team__team_name').annotate(Max('score_1')))
    s2 = list(SubmissionAssignmentOne.objects.values(
        'team__team_name').annotate(Max('score_2')))
    s3 = list(SubmissionAssignmentOne.objects.values(
        'team__team_name').annotate(Max('score_3')))
    s4 = list(SubmissionAssignmentOne.objects.values(
        'team__team_name').annotate(Max('score_4')))

    all_rows = []
    team_usn_mapping = {}
    csv_file = csv.reader(open('new_bd_resp.csv', "r"), delimiter=",")
    for row in csv_file:
        print(row)
        team_usn_mapping[row[24]] = []
    csv_file = csv.reader(open('new_bd_resp.csv', "r"), delimiter=",")
    for row in csv_file:
        team_usn_mapping[row[24]].append([row[1], row[2], row[3]])
        team_usn_mapping[row[24]].append([row[5], row[6], row[7]])
        if row[10] != '':
            team_usn_mapping[row[24]].append([row[10], row[11], row[12]])
        if row[15] != '':
            team_usn_mapping[row[24]].append([row[15], row[16], row[17]])

    for i in range(len(s1)):
        try:
            usn_list = team_usn_mapping[s1[i]['team__team_name']]
            print("team exists")
        except Exception as e:
            print("team does not exist")
            continue
        for usn in usn_list:
            print(usn)
            all_rows.append([usn[0], usn[2],usn[1], s1[i]['score_1__max'],
                            s2[i]['score_2__max'], s3[i]['score_3__max'],
                            s4[i]['score_4__max'], s1[i]['team__team_name']])
    with open('individual_marks_test.csv', 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(all_rows)
    csvFile.close()
