from django.core.exceptions import ObjectDoesNotExist
from django.http import HttpResponse
from django.shortcuts import render
from django.views import View

from .models import SubmissionAssignmentOne, SubmissionAssignmentTwo, Team
from .utils import run_assignment_one, run_assignment_two


# noinspection PyMethodMayBeStatic
class CodeUploadAssignmentOne(View):
    def get(self, request):
        return render(request, 'assignment1.html')

    def post(self, request):
        try:
            t_name = request.POST["team_name"]
            python = True if request.POST["python_or_java"] == "python" else False
            java = True if request.POST["python_or_java"] == "java" else False
            code_config = request.FILES["code_config"]
            if java:
                code_file_java_task_1 = request.FILES["code_file_java_task_1"]
                code_file_java_task_2 = request.FILES["code_file_java_task_2"]
                code_file_java_task_3 = request.FILES["code_file_java_task_3"]
                code_file_python_map_1 = None
                code_file_python_map_2 = None
                code_file_python_map_3 = None
                code_file_python_reduce_1 = None
                code_file_python_reduce_2 = None
                code_file_python_reduce_3 = None
            if python:
                code_file_python_map_1 = request.FILES["code_file_python_map_1"]
                code_file_python_map_2 = request.FILES["code_file_python_map_2"]
                code_file_python_map_3 = request.FILES["code_file_python_map_3"]
                code_file_python_reduce_1 = request.FILES["code_file_python_reduce_1"]
                code_file_python_reduce_2 = request.FILES["code_file_python_reduce_2"]
                code_file_python_reduce_3 = request.FILES["code_file_python_reduce_3"]
                code_file_java_task_1 = None
                code_file_java_task_2 = None
                code_file_java_task_3 = None
        except IndexError as e:
            return HttpResponse("Unable to accept submission. Enter valid details")
        try:
            team = Team.objects.get(team_name=t_name)
        except ObjectDoesNotExist as e:
            return HttpResponse(
                "Team doesnt exist. Enter the team name submitted in the project form. If you dont remember your"
                + " team name, contact mayankthakur@pesu.pes.edu or any faculty immediately")
        try:
            submission = SubmissionAssignmentOne(
                team=team,
                python=python,
                java=java,
                code_config=code_config,
                code_file_java_task_1=code_file_java_task_1,
                code_file_java_task_2=code_file_java_task_2,
                code_file_java_task_3=code_file_java_task_3,
                code_file_python_map_1=code_file_python_map_1,
                code_file_python_map_2=code_file_python_map_2,
                code_file_python_map_3=code_file_python_map_3,
                code_file_python_reduce_1=code_file_python_reduce_1,
                code_file_python_reduce_2=code_file_python_reduce_2,
                code_file_python_reduce_3=code_file_python_reduce_3
            )
            submission.save()
        except Exception as e:
            print(e)
            return HttpResponse("Cannot accept this submission. Please submit valid data")
        try:
            run_assignment_one.apply_async([submission.id])
        except Exception as e:
            print(e)
            return HttpResponse("Cannot add submission to queue. Contact mayankthakur@pesu.pes.edu for resolution")
        return HttpResponse(
            "Done. You will receive your results via an email. The submission id is " +
            str(submission.id)
            + ". Use this submission id to get in touch in case you dont get an email result.")


class CodeUploadAssignmentTwo(View):
    def get(self, request):
        try:
            return render(request, 'assignment2.html')
        except Exception as e:
            return HttpResponse("Could not render. Contact mayankthakur@pesu.pes.edu")

    def post(self, request):
        try:
            t_name = request.POST["team_name"]
            code_file_task_1 = request.FILES["code_file_task_1"]
            code_file_task_2 = request.FILES["code_file_task_2"]
        except IndexError as e:
            return HttpResponse("Unable to accept submission. Enter valid details")
        try:
            team = Team.objects.get(team_name=t_name)
        except ObjectDoesNotExist as e:
            return HttpResponse(
                "Team doesnt exist. Enter the team name submitted in the project form. If you dont remember your"
                + " team name, contact mayankthakur@pesu.pes.edu or any faculty immediately")
        try:
            submission = SubmissionAssignmentTwo(
                team=team,
                code_file_task_1=code_file_task_1,
                code_file_task_2=code_file_task_2
            )
            submission.save()
        except Exception as e:
            print(e)
            return HttpResponse("Could not accept submission, enter valid details")
        try:
            run_assignment_two(submission.id)
        except Exception as e:
            print(e)
            return HttpResponse("Cannot push submission in the queue")
                 