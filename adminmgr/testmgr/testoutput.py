import os
from hdfs import InsecureClient
from .config import *


def test_task_3(output_paths):
    if(output_paths[0] == None):
        return [0]
    path_to_correct_output_1 = os.path.join(SETTERS_OUTPUT_BASE_PATH, 
                                          "Task3",
                                          "0.txt")
    path_to_correct_output_2 = os.path.join(SETTERS_OUTPUT_BASE_PATH, 
                                          "Task3",
                                          "1.txt")
    x_1 = os.system("diff -wBZ "+path_to_correct_output_1 + " " + output_paths[0] + "> abc")
    x_2 = os.system("diff -wBZ "+path_to_correct_output_2 + " " + output_paths[0] + "> abc")
    
    if x_1 == 0 or x_2 == 0:
        return [4]
    return [0]
  
def test_task_1_2(output_paths, task_number):
    path_to_correct_output_1 = os.path.join(SETTERS_OUTPUT_BASE_PATH, 
                                          "Task"+task_number,
                                          "0.txt")
    path_to_correct_output_2 = os.path.join(SETTERS_OUTPUT_BASE_PATH, 
                                          "Task"+task_number,
                                          "1.txt")
    path_to_correct_output_3 = os.path.join(SETTERS_OUTPUT_BASE_PATH, 
                                          "Task"+task_number,
                                          "2.txt")
    path_to_correct_output_4 = os.path.join(SETTERS_OUTPUT_BASE_PATH, 
                                          "Task"+task_number,
                                          "3.txt")
    x_1 = os.system("diff -wBZ "+path_to_correct_output_1 + " " + output_paths[0] + "> abc")
    if x_1 == 0:
        return [4]
    x_2 = os.system("diff -wBZ "+path_to_correct_output_2 + " " + output_paths[0] + "> abc")
    if x_2 == 0:
        return [3]
    x_3 = os.system("diff -wBZ "+path_to_correct_output_3 + " " + output_paths[0] + "> abc")
    if x_3 == 0:
        return [2]
    x_4 = os.system("diff -wBZ "+path_to_correct_output_4 + " " + output_paths[0] + "> abc")
    if x_4 == 0:
        return [1]
    return [0]
        
def test(output_paths, task_number):
    correctness = []	
    
    for output_path in output_paths:
        scores = []
        if output_path is None:
            print("SKIPPING TEST CASE ")
            continue
        print(output_path)
        test_case = output_paths.index(output_path) + 1
        for i in range(4):
            score = check_test_case(output_path,
                                   (os.path.join(SETTERS_OUTPUT_BASE_PATH, "Task"+task_number,
                                   str(i) + ".txt")), str(i))
            if(score != 0):
                scores.append(score)
                break
        if(len(scores) == 0):
            correctness.append(0)
        else:
            correctness.append(scores[0])
    print("CORRECTNESS")
    print(correctness)
    return correctness


def check_test_case(path_to_team_output, path_to_correct_output,
                    test_case_number):
    print("team_output = " + path_to_team_output)
    print("setters_output = " + path_to_correct_output)
        
    score = 4 - int(test_case_number)
    x = os.system("diff -wBZ "+path_to_team_output+" "+path_to_correct_output + " > abc")
    print("x = "+str(x))
    if x == 0:
        print("TEST CASE " + str(test_case_number) + " PASSED")
        return score
    else:
        return 0
