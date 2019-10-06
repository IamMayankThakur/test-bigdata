from api.models import SubmissionAssignmentTwo
from .config import *
import errno
import os


def execute_test(code_path, input_path, result_path, setters_path, iterations):
    # command = "/opt/spark/spark-2.4.4-bin-hadoop2.7/bin/spark-submit"

    command = SPARK_BASE_PATH
    command += " " + code_path
    command += " " + input_path
    command += " " + str(iterations)
    command += " " + ">>" + " " + result_path + "/op.txt"
    message = ""
    print("command is " + command)
    print(os.getcwd())
    try:
        x = os.system(command)
    except Exception as e:
        message += "Error compiling code" + " "
        return [message, 0]

    message += "Executed successfully " + " "

    score = 0
    x = os.system("diff -wBZ " + result_path+"/op.txt" + " " + setters_path)
    if x == 0:
        score = 1
    else:
        score = 0
    return [message, score]


def execute_test_tasks(code_path, task_number, n_test_cases, output_path):
    message = ""
    scores = []
    iterations = [[18, 23], [28, 18]]
    for i in range(n_test_cases):
        message += "Test case " + str(i) + "\n"
        result_path = os.path.join(output_path, str(i))
        input_path = os.path.join(BASE_PATH, DATASET_BASE_PATH, str(task_number), str(i) + ".txt")
        setters_path = os.path.join(BASE_PATH, SETTERS_BASE_PATH, str(task_number), str(i) + ".txt")
        os.makedirs(result_path, exist_ok=True)
        output = execute_test(code_path, input_path, result_path, setters_path, iterations[task_number - 1][i])
        message += output[0]
        if output[1] == 0:
            message += "Failed test case\n"
        else:
            message += "Passed test case\n"
        scores.append(output[1])
    return [message, scores]


def exe(submission_id):
    submission = SubmissionAssignmentTwo.objects.get(id=submission_id)
    mail_message_1 = "Task 1\n"
    mail_message_2 = "Task 2\n"
    score_1 = 0
    score_2 = 0

    output_path = os.path.join(TEAM_BASE_PATH, submission.team.team_name, str(submission_id))
    print("Output path is " + output_path)
    try:
        os.makedirs(output_path, exist_ok=True)
        print("CREATED " + submission.team.team_name + " DIRECTORY")
    except OSError as e:
        if e.errno != errno.EEXIST:
            print(e)
            print("Error creating team folder")
            return -1
        else:
            print("WARNING - DIRECTORY ALREADY EXISTS")
            pass

    output = execute_test_tasks(
        submission.code_file_task_1.path, 1, 2, output_path)
    mail_message_1 += output[0]
    score_1 = sum(output[1])

    output = execute_test_tasks(
        submission.code_file_task_2.path, 2, 2, output_path)
    mail_message_2 += output[0]
    score_2 = sum(output[1])

    submission.score_1 = score_1
    submission.score_2 = score_2
    submission.remarks = mail_message_1 + "\n" + mail_message_2 + "\n"
    submission.save()