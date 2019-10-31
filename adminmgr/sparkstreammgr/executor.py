from api.models import SubmissionAssignmentThree
from .config import *
import errno
import os
import datetime
import time


def execute_task1(submission):
    print("Executing Task1 Subtask 1")
    output_path = os.path.join(
        TEAM_BASE_PATH, submission.team.team_name, str(submission.id))
    message = ""
    scores = []
    message += "Subtask 1 <br>"
    result_path = os.path.join(output_path, "1", "1")
    setters_path = os.path.join(
        BASE_PATH, SETTERS_BASE_PATH, "t1s1.txt")
    setters_path_2 = os.path.join(
        BASE_PATH, SETTERS_BASE_PATH, "t1s1_2.txt")
    os.makedirs(result_path, exist_ok=True)
    runcmd = "python3.6 "+submission.code_file_task_1.path + \
        " > "+result_path+"/t1s1.output"
    diffcmd = "diff -wBZ "+result_path+"/t1s1.output "+setters_path
    diffcmd2 = "diff -wBZ "+result_path+"/t1s1.output "+setters_path_2
    try:
        run_code = os.system(runcmd)
        print("T1S1", runcmd)
        # run_code = 0
        if run_code != 0:
            message += "Runtime Error<br>"
            scores.append(0)
        else:
            diff_code = os.system(diffcmd)
            diff_code2 = os.system(diffcmd2)
            print("DT1S1", diffcmd)
            # diff_code = 0
            if diff_code != 0 and diff_code2 != 0:
                message += "Wrong Output<br>"
                scores.append(0)
            else:
                message += "Correct Output<br>"
                scores.append(1)
    except Exception as e:
        print(e)
        message += "Unknown Error <br>"
        scores.append(0)
    print("Executing Task 1 Subtask 2")
    message += "Subtask 2 <br>"
    result_path = os.path.join(output_path, "1", "2")
    setters_path = os.path.join(
        BASE_PATH, SETTERS_BASE_PATH, "t1s2.txt")
    os.makedirs(result_path, exist_ok=True)
    runcmd = "python3.6 "+submission.code_file_task_1.path + \
        " > "+result_path+"/t1s2.output"
    diffcmd = "diff -wBZ "+result_path+"/t1s2.output "+setters_path
    try:
        run_code = os.system(runcmd)
        print("T1S2", runcmd)
        # run_code = 0
        if run_code != 0:
            message += "Runtime Error<br>"
            scores.append(0)
        else:
            diff_code = os.system(diffcmd)
            print("DT1S2", diffcmd)
            # diff_code = 0
            if diff_code != 0:
                message += "Wrong Output<br>"
                scores.append(0)
            else:
                message += "Correct Output<br>"
                scores.append(1)
    except Exception as e:
        print(e)
        message += "Unknown Error <br>"
        scores.append(0)
    return [message, scores]


def execute_task2(submission):
    print("Executing Task 2 Subtask 1")
    output_path = os.path.join(
        TEAM_BASE_PATH, submission.team.team_name, str(submission.id))
    message = ""
    scores = []
    message += "Subtask 1 <br>"
    result_path = os.path.join(output_path, "2", "1")
    setters_path = os.path.join(
        BASE_PATH, SETTERS_BASE_PATH, "t2s1.txt")
    setters_path_2 = os.path.join(
        BASE_PATH, SETTERS_BASE_PATH, "t2s1_2.txt")
    os.makedirs(result_path, exist_ok=True)
    runappcmd = "python3 ~/app.py &"
    runcmd = "python3.6 "+submission.code_file_task_1.path + " 2 1" \
        " > "+result_path+"/t2s1.output"
    diffcmd = "diff -wBZ "+result_path+"/t2s1.output "+setters_path
    diffcmd2 = "diff -wBZ "+result_path+"/t2s1.output "+setters_path_2
    try:
        run_app = os.system(runappcmd)
        time.sleep(5)
        run_code = os.system(runcmd)
        print("T2S1", runcmd)
        time.sleep(10)
        # run_code = 0
        if run_code != 0:
            message += "Runtime Error<br>"
            scores.append(0)
        else:
            diff_code = os.system(diffcmd)
            diff_code2 = os.system(diffcmd2)
            print("DT2S1", diffcmd)
            # diff_code = 0
            if diff_code != 0 and diff_code2 != 0:
                message += "Wrong Output<br>"
                scores.append(0)
            else:
                message += "Correct Output<br>"
                scores.append(1)
    except Exception as e:
        print(e)
        message += "Unknown Error <br>"
        scores.append(0)

    print("Executing Task 2 Subtask 2")
    message += "Subtask 2 <br>"
    result_path = os.path.join(output_path, "2", "2")
    setters_path = os.path.join(
        BASE_PATH, SETTERS_BASE_PATH, "t2s2.txt")
    setters_path_2 = os.path.join(
        BASE_PATH, SETTERS_BASE_PATH, "t2s2_2.txt")
    os.makedirs(result_path, exist_ok=True)
    runappcmd = "python3 ~/app.py &"
    runcmd = "python3.6 "+submission.code_file_task_1.path + " 1 1" \
        " > "+result_path+"/t2s2.output"
    diffcmd = "diff -wBZ "+result_path+"/t2s2.output "+setters_path
    diffcmd2 = "diff -wBZ "+result_path+"/t2s2.output "+setters_path_2
    try:
        run_app = os.system(runappcmd)
        time.sleep(5)
        run_code = os.system(runcmd)
        time.sleep(10)
        print("T2S2", runcmd)
        # run_code = 0
        if run_code != 0:
            message += "Runtime Error<br>"
            scores.append(0)
        else:
            diff_code = os.system(diffcmd)
            diff_code2 = os.system(diffcmd2)
            print("DT2S2", diffcmd)
            # diff_code = 0
            if diff_code != 0 and diff_code2 != 0:
                message += "Wrong Output<br>"
                scores.append(0)
            else:
                message += "Correct Output<br>"
                scores.append(1)
    except Exception as e:
        print(e)
        message += "Unknown Error <br>"
        scores.append(0)
    return [message, scores]


def execute(submission_id):
    submission = SubmissionAssignmentThree.objects.get(id=submission_id)
    mail_message_1 = "Task 1<br>"
    mail_message_2 = "Task 2<br>"
    score_1 = 0
    score_2 = 0
    output_path = os.path.join(
        TEAM_BASE_PATH, submission.team.team_name, str(submission_id))
    try:
        os.makedirs(output_path, exist_ok=True)
    except OSError as e:
        if e.errno != errno.EEXIST:
            print(e)
            print("Error creating team folder")
            return -1
        else:
            print("WARNING - DIRECTORY ALREADY EXISTS")
            pass

    output = execute_task1(submission)
    mail_message_1 += output[0] + "<br>"
    score_1 = sum(output[1])
    output = execute_task2(submission)
    mail_message_2 += output[0] + "<br>"
    score_2 = sum(output[1])

    submission.score_1 = score_1
    submission.score_2 = score_2
    if score_1 == 2 and score_2 == 2:
        team = submission.team
        team.a3_full = True
        team.save()
    submission.remarks = mail_message_1 + "<br> <br>" + mail_message_2
    submission.save()
