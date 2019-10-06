import os
def execute_test(code_path, input_path, output_path):
    command = "" + code_path + " "
    command += input_path
    message = ""
    try:
        x = os.system(command)
    except Exception as e:
        message += "Error compiling code"
        return message

    if x!=0:


def execute_test_tasks(code_path, task_number, n_test_cases):
    input_path = "" + task_number
    message = ""
    for i in range(len(n_test_cases)):
        message += execute_test(code_path, input_path, output_path)


def exe(submission_id):
    submission = SubmissionAssignmentTwo.objects.get(id=submission_id)
    mail_message_1 = "Task 1\n"
    mail_message_2 = "Task 2\n"
    score_1 = 0
    score_2 = 0

    mail_message_1 += execute_test_task(submission.code_file_python_task_1.path, 1, 2)
