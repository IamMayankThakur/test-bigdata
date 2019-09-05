import errno
import os
import time

from hdfs import InsecureClient

from api.models import SubmissionAssignmentOne
from .config import *
from .jarfilegenerator import java_jar_file_generator
from .mapreducecodecompiler import java_map_reduce_compile
from .mapreduceexecutor import java_map_reduce_execute
from .mapreduceexecutor import python_map_reduce_execute
from .testoutput import test
from .testoutput import test_task_1_2
from .testoutput import test_task_3


def download_file(path, test_case_number, task_number):
    try:
        client = InsecureClient(('http://'
                                 + HADOOP_HOST_NAME + ':'
                                 + HADOOP_NAMENODE_PORT_NUMBER), user=HADOOP_USER_NAME)
    except:
        print("Error connecting to hdfs client")
        return
    try:
        client.download(HADOOP_OUTPUT_PATH+task_number+test_case_number + "/", os.path.join(path, test_case_number))
    except Exception as e:
        print(e)
        print("Error downloading output file from hdfs")
        return
    try:
        client.delete(HADOOP_OUTPUT_PATH+task_number+test_case_number, recursive=True)
    except:
        print("Error deleting hdfs output directory")
        return


def process_java(path_to_code, team_name, task_number):
    if(task_number == '1'):
        if(os.path.exists(os.path.join(TEAMS_BASE_PATH, team_name))):
            print("TEAM ALREADY EXISTS "+team_name+" - DELETING PREVIOUS SUBMISSION")
            os.system("rm -rf "+os.path.join(TEAMS_BASE_PATH, team_name))
    team_folder_path = os.path.join(TEAMS_BASE_PATH, team_name, task_number)
    try:
        os.makedirs(team_folder_path, exist_ok=True)
        print("CREATED "+team_name+" DIRECTORY")
    except OSError as e:
        if e.errno != errno.EEXIST:
            print(e)
            print("Error creating team folder")
            return -1
        else: 
            print("WARNING - DIRECTORY ALREADY EXISTS")
            pass

    if not java_map_reduce_compile(path_to_code, team_folder_path):
        return -2
    print("[TEST-COMPONENT-LOG][" + team_name + "] COMPILATION SUCCESSFUL")

    if (java_jar_file_generator(os.path.join(team_folder_path, "ClassFiles"),
                                team_folder_path, "test") is False):
        return -3
    print("[TEST-COMPONENT-LOG][" + "/" + team_name + "] JAR FILE CREATED")
    return team_folder_path


def java_execute_test_cases(path_to_code, team_name, task_number):
    team_folder_path = process_java(path_to_code, team_name, task_number)
    if isinstance(team_folder_path, int):
        if team_folder_path < 0:
            print("Processing java code failed")
            return [team_folder_path]
    output_paths = []
    for test_case in range(1, TEST_CASES + 1):
        output_paths.append(execute_java(team_name, team_folder_path, str(test_case), task_number))
    return output_paths


def python_execute_test_cases(path_to_mapper,
                              path_to_reducer,
                              team_name,
                              task_number):
    if(task_number == '1'):
        if(os.path.exists(os.path.join(TEAMS_BASE_PATH, team_name))):
            print("TEAM ALREADY EXISTS "+team_name+" - DELETING PREVIOUS SUBMISSION")
            os.system("rm -rf "+os.path.join(TEAMS_BASE_PATH, team_name))  
    output_paths = []
    team_folder_path = os.path.join(TEAMS_BASE_PATH, team_name, task_number)
    try:
        os.makedirs(team_folder_path, exist_ok=True)
        print("CREATED "+team_name+" DIRECTORY")
    except OSError as e:
        if e.errno != errno.EEXIST:
            print(e)
            print("Error creating team folder")
            return None
        else:
            print("WARNING - DIRECTORY ALREADY EXISTS")
            pass
    for test_case in range(1, TEST_CASES + 1):
        output_paths.append(execute_python(path_to_mapper,
                                           path_to_reducer,
                                           team_folder_path,
                                           str(test_case),
                                           task_number))
    return output_paths


def execute_python(path_to_mapper, path_to_reducer, team_folder_path, test_case_number, task_number):
    if python_map_reduce_execute(path_to_mapper, path_to_reducer, "/Test_Case_" + str(test_case_number), HADOOP_OUTPUT_PATH+task_number+test_case_number) is False:
        return None
    print("[TEST-COMPONENT-LOG]MAP REDUCE EXECUTION SUCCESSFUL")
    download_file(team_folder_path, test_case_number, task_number)
    return os.path.join(team_folder_path, test_case_number, "part-00000")


def execute_java(team_name, team_folder_path, test_case_number, task_number):
    if (java_map_reduce_execute(os.path.join(team_folder_path, "test.jar"),
                                team_name, "/Test_Case_" + str(test_case_number),
                                HADOOP_OUTPUT_PATH+task_number+test_case_number) is False):
        return None
    print("[TEST-COMPONENT-LOG]MAP REDUCE EXECUTION SUCCESSFUL")

    download_file(team_folder_path, test_case_number, task_number)
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
        print("JAVA STARTING TASK 1")
        output_paths = java_execute_test_cases(submission.code_file_java_task_1.path, submission.team.team_name, '1')
        if output_paths[0] == -1:
            mail_message_1 += "unknown error\n"
        elif output_paths[0] == -2:
            mail_message_1 += "compilation error for task\n"
        elif output_paths[0] == -3:
            mail_message_1 += "jar file generation issue\n"
        else:
            correctness = test_task_1_2(output_paths, '1')
            for i in range(len(correctness)):
                score_1 += correctness[i]
        print("JAVA STARTING TASK 2")
        output_paths = java_execute_test_cases(submission.code_file_java_task_2.path, submission.team.team_name, '2')
        if output_paths[0] == -1:
            mail_message_2 += "unknown error\n"
        elif output_paths[0] == -2:
            mail_message_2 += "compilation error for task\n"
        elif output_paths[0] == -3:
            mail_message_2 += "jar file generation issue\n"        
        else:
            correctness = test_task_1_2(output_paths, '2')
            for i in range(len(correctness)):
                score_2 += correctness[i]
        print("JAVA STARTING TASK 3")
        output_paths = java_execute_test_cases(submission.code_file_java_task_3.path, submission.team.team_name, '3')
        if not output_paths:
            mail_message_3 += "Compilation error\n"
        else:
            correctness = test_task_3(output_paths)
            for i in range(len(correctness)):
                score_3 += correctness[i]


    if submission.python:
        print("PYTHON STARTING TASK 1")
        output_paths = python_execute_test_cases(submission.code_file_python_map_1.path, submission.code_file_python_reduce_1.path, submission.team.team_name, '1')
        print(output_paths)
        if not output_paths or output_paths[0] == None:
            mail_message_1 += "Compilation error\n"
        else:
            correctness = test_task_1_2(output_paths, '1')
            for i in range(len(correctness)):
                score_1 += correctness[i]
        print("PYTHON STARTING TASK 2")
        output_paths = python_execute_test_cases(submission.code_file_python_map_2.path, submission.code_file_python_reduce_2.path, submission.team.team_name, '2')
        if not output_paths or output_paths[0] == None:
            mail_message_2 += "Compilation error\n"
        else:
            correctness = test_task_1_2(output_paths, '2')
            for i in range(len(correctness)):
                score_2 += correctness[i]
        print("PYTHON STARTING TASK 3")
        output_paths = python_execute_test_cases(submission.code_file_python_map_3.path, submission.code_file_python_reduce_3.path, submission.team.team_name, '3')
        if not output_paths or output_paths[0] == None:
            mail_message_3 += "Compilation error\n"
        else:
            correctness = test_task_3(output_paths)
            for i in range(len(correctness)):
                score_3 += correctness[i]

      

    if(mail_message_1 != "Task 1\n"):
        submission.remarks = mail_message_1 + '\n\n' 
    if(mail_message_2 != "Task 2\n"):
        submission.remarks += mail_message_2 + '\n\n' 
    if(mail_message_3 != "Task 3\n"):
        submission.remarks += mail_message_3 + "\n\n"
    submission.score_1 = 4
    submission.score_2 = score_1
    submission.score_3 = score_2
    submission.score_4 = score_3
    submission.save()
