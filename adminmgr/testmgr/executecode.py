import errno
import os

from hdfs import InsecureClient

from api.models import SubmissionAssignmentOne
from .config import *
from .jarfilegenerator import java_jar_file_generator
from .mapreducecodecompiler import java_map_reduce_compile
from .mapreduceexecutor import java_map_reduce_execute
from .mapreduceexecutor import python_map_reduce_execute
from .testoutput import test


def download_file(path, test_case_number):
    try:
        client = InsecureClient(('http://'
                                 + HADOOP_HOST_NAME + ':'
                                 + HADOOP_NAMENODE_PORT_NUMBER), user=HADOOP_USER_NAME)
    except:
        print("Error connecting to hdfs client")
        return
    try:
        client.download(HADOOP_OUTPUT_PATH + "/", os.path.join(path, test_case_number))
    except:
        print("Error download output file")
        return
    try:
        client.delete(HADOOP_OUTPUT_PATH, recursive=True)
    except:
        print("Error deleting output directory")
        return


def process_java(path_to_code, team_name):
    team_folder_path = os.path.join(TEAMS_BASE_PATH, team_name)
    try:
        os.mkdir(team_folder_path)
        print("Created team folder")
    except OSError as e:
        if e.errno != errno.EEXIST:
            return -1
        else:
            pass

    if not java_map_reduce_compile(path_to_code, team_folder_path):
        return -2
    print("[TEST-COMPONENT-LOG][" + team_name + "] COMPILATION SUCCESSFUL")

    if (java_jar_file_generator(os.path.join(team_folder_path, "ClassFiles"),
                                team_folder_path, "WordCountJ") is False):
        return -3
    print("[TEST-COMPONENT-LOG][" + "/" + team_name + "] JAR FILE CREATED")
    return team_folder_path


def java_execute_test_cases(path_to_code, team_name):
    team_folder_path = process_java(path_to_code, team_name)
    if team_folder_path < 0:
        print("Processing java code failed")
        return team_folder_path
    output_paths = []
    for test_case in range(1, TEST_CASES + 1):
        output_paths.append(execute_java(path_to_code, team_folder_path, str(test_case)))
    return output_paths


def python_execute_test_cases(path_to_mapper,
                              path_to_reducer,
                              team_name):
    output_paths = []
    team_folder_path = os.path.join(TEAMS_BASE_PATH, team_name)
    try:
        os.mkdir(team_folder_path)
        print("Created team folder")
    except OSError as e:
        if e.errno != errno.EEXIST:
            return -1
        else:
            pass
    for test_case in range(1, TEST_CASES + 1):
        output_paths.append(execute_python(path_to_mapper,
                                           path_to_reducer,
                                           team_folder_path,
                                           str(test_case)))
    return output_paths


def execute_python(path_to_mapper, path_to_reducer, team_folder_path, test_case_number):
    if (python_map_reduce_execute(path_to_mapper, path_to_reducer, "/Input/alldata.csv", "/output_word") is False):
        return None
    print("[TEST-COMPONENT-LOG]MAP REDUCE EXECUTION SUCCESSFUL")
    download_file(team_folder_path, test_case_number)
    return os.path.join(team_folder_path, test_case_number, "part-00000")


def execute_java(path_to_code, team_folder_path, test_case_number):
    if (java_map_reduce_execute(os.path.join(team_folder_path, "test.jar"),
                                "Task1", "/Test_Case_" + str(test_case_number),
                                HADOOP_OUTPUT_PATH) is False):
        return None
    print("[TEST-COMPONENT-LOG]MAP REDUCE EXECUTION SUCCESSFUL")

    download_file(team_folder_path, test_case_number)
    return os.path.join(team_folder_path, test_case_number, "part-r-00000")


def exe(submission_id):
    submission = SubmissionAssignmentOne.objects.get(id=submission_id)
    mail_message_1 = "Task 1\n"
    mail_message_2 = "Task 2\n"
    mail_message_3 = "Task 3\n"
    score_1 = 0
    score_2 = 0
    score_3 = 0
    if submission.java:
        output_paths = java_execute_test_cases(submission.code_file_java_task_1.path, submission.team.team_name)
        if (output_paths == -1):
            mail_message_1 += "unknown error\n"
        elif (output_paths == -2):
            mail_message_1 += "compilation error for task\n"
        elif (output_paths == -3):
            mail_message_1 += "jar file generation issue\n"
        # else:
        #     correctness = test(output_paths, '1')
        #     for i in range(len(correctness)):
        #         mail_message_1 += "Test case " + str(i) + " "
        #         if (correctness[i] == 1):
        #             mail_message_1 += "Passed\n"
        #             score_1 += 1
        #         else:
        #             mail_message_1 += "Failed\n"

        output_paths = java_execute_test_cases(submission.code_file_java_task_1.path, submission.team.team_name)
        if (output_paths == -1):
            mail_message_2 += "unknown error"
        elif (output_paths == -2):
            mail_message_2 += "compilation error"
        elif (output_paths == -3):
            mail_message_2 += "jar file generation error"
        # else:
        #     correctness = test(output_paths, '1')
        #     for i in range(len(correctness)):
        #         mail_message_2 += "Test case " + str(i) + " "
        #         if (correctness[i] == 1):
        #             mail_message_2 += "Passed\n"
        #             score_2 += 1
        #         else:
        #             mail_message_2 += "Failed\n"

        output_paths = java_execute_test_cases(submission.code_file_java_task_1.path, submission.team.team_name)
        print(output_paths)
        if (output_paths == -1):
            mail_message_3 += "unknown error"
        elif (output_paths == -2):
            mail_message_3 += "compilation error for task"
        elif (output_paths == -3):
            mail_message_3 += "jar file generation issue"
        # else:
        #     correctness = test(output_paths, '1')
        #     for i in range(len(correctness)):
        #         mail_message_3 += "Test case " + str(i) + " "
        #         if (correctness[i] == 1):
        #             mail_message_3 += "Passed\n"
        #             score_3 += 1
        #         else:
        #             mail_message_3 += "Failed\n"

    if submission.python:
        output_paths = python_execute_test_cases(submission.code_file_java_task_1.path, submission.team.team_name)
        if (output_paths[0] == False):
            mail_message_1 += "Compilation error\n"
        else:
            correctness = test(output_paths, '1')
            for i in range(len(correctness)):
                mail_message_1 += "Test case " + str(i) + " "
                if (correctness[i] == 1):
                    mail_message_1 += "Passed\n"
                    score_1 += 1
                else:
                    mail_message_1 += "Failed\n"

        output_paths = python_execute_test_cases(submission.code_file_java_task_1.path, submission.team.team_name)
        if (output_paths[0] == False):
            mail_message_2 += "Compilation error\n"
        else:
            correctness = test(output_paths, '1')
            for i in range(len(correctness)):
                mail_message_2 += "Test case " + str(i) + " "
                if (correctness[i] == 1):
                    mail_message_2 += "Passed\n"
                    score_2 += 1
                else:
                    mail_message_2 += "Failed\n"

        output_paths = python_execute_test_cases(submission.code_file_java_task_1.path, submission.team.team_name)
        if (output_paths[0] == False):
            mail_message_3 += "Compilation error\n"
        else:
            correctness = test(output_paths, '1')
            for i in range(len(correctness)):
                mail_message_3 += "Test case " + str(i) + " "
                if (correctness[i] == 1):
                    mail_message_3 += "Passed\n"
                    score_3 += 1
                else:
                    mail_message_3 += "Failed\n"

    submission.remarks = mail_message_1 + ';' + mail_message_2 + ';' + mail_message_3
    submission.score_1 = score_1
    submission.score_2 = score_2
    submission.score_3 = score_3
    submission.save()
