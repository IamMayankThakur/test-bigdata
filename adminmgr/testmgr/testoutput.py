import os
from hdfs import InsecureClient
from .config import *


def test(output_paths, task_number):
    correctness = []
    for output_path in output_paths:
        if output_path is None:
            print("SKIPPING TEST CASE ")
            continue
        print(output_path)
        test_case = output_paths.index(output_path) + 1
        correctness.append(check_test_case(output_path,
                                           (os.path.join(SETTERS_OUTPUT_BASE_PATH, task_number,
                                                         str(test_case) + ".txt")), str(test_case)))
    return correctness


def check_test_case(path_to_team_output, path_to_correct_output,
                    test_case_number):
    print("team_output = " + path_to_team_output)
    print("setters_output = " + path_to_correct_output)
    team_output = open(path_to_team_output, 'r').read()
    correct_output = open(path_to_correct_output, 'r').read()
    if team_output == correct_output:
        print("TEST CASE " + str(test_case_number) + " PASSED")
        return 1
    else:
        print("TEST CASE " + str(test_case_number) + " FAILED")
        return 0
