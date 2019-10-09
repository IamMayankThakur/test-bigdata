import pdb
from django.db.models import Max
from api.models import SubmissionAssignmentOne, Team
import csv
import pandas as pd
from celery import shared_task

from notifymgr.mail import send_mail, _send
# from testmgr.executecode import exe
from sparktestmgr.sparkcodeexecutor import exe
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


@shared_task
def run_assignment_one(submission_id):
    # exe(submission_id=submission_id)
    send_mail(submission_id, 1)
    print("Submission Evaluation Complete")


@shared_task
def run_assignment_two(submission_id):
    exe(submission_id=submission_id)
    send_mail(submission_id, 2)
    print("Submisssion Evaluation Complete")


def port_csv_to_db():
    csv_path = "/home/ubuntu/test-bigdata/data_reg.csv"
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
            all_rows.append([usn[0], usn[2], usn[1], s1[i]['score_1__max'],
                             s2[i]['score_2__max'], s3[i]['score_3__max'],
                             s4[i]['score_4__max'], s1[i]['team__team_name']])
    with open('individual_marks_test.csv', 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(all_rows)
    csvFile.close()


def send_plagiarism_mail():
    ass1_list = ['BD_1385_1602_1667_1771',
                 'BD_135_703_2371',
                 'BD_252_243_55_759',
                 'BD_051_272_1339',
                 'BD_096_648_973_1910',
                 'BD_0912_1279_1350',
                 'BD_1580_740_780',
                 'BD_0079_0954_1125',
                 'BD_0012_0792_0948_1324',
                 'BD_133_223_1002_1128',
                 'BD_0160_1274_1540_1801',
                 'BD_228_278_1110_1593',
                 'BD_167_260_770_788',
                 'BD_167_260_770_788',
                 'BD_167_260_770_788',
                 'BD_665_962_725',
                 'BD_85_130_185_279',
                 'BD_665_1113_1809',
                 'BD_830_1094_1439_2391',
                 'BD_1595_1702_0194',
                 'BD_228_278_1110_1593',
                 'BD_85_130_185_279',
                 'BD_85_130_185_279',
                 'BD_1595_1702_0194',
                 'BD_62_1136',
                 'BD_85_130_185_279',
                 'BD_830_1094_1439_2391',
                 'BD_498_957_959',
                 'BD_1348_1576_1597',
                 'BD_444_459_489']
    message = MIMEMultipart("alternative")
    message["Subject"] = "Meet KVS Sir"
    message["From"] = "noreplybigdata@gmail.com"

    html = "Hi, <br> Please meet KVS Sir ASAP without fail. This is in lieu of the plagiarism report. It is better to meet him with your complete team along with the team you had a plagiarism match with."

    part = MIMEText(html, "html")
    message.attach(part)
    _send("mayankthakur@pesu.pes.edu", message)
    for team_name in ass1_list:
        try:
            team = Team.objects.get(team_name=team_name)
            emails = [team.member_1, team.member_2,
                      team.member_3, team.member_4]
            for email in emails:
                if email != 'nan':
                    _send(email, message)
        except:
            pass
