from django.core.exceptions import ObjectDoesNotExist
from django.http import HttpResponse
from django.shortcuts import render
from django.views import View

from .models import SubmissionAssignmentOne
from .models import Team
from .utils import run


# noinspection PyMethodMayBeStatic
class CodeUpload(View):
    def get(self, request):
        return render(request, 'index.html')

    def post(self, request):
        try:
            t_name = request.POST["team_name"]
            python = True if request.POST["python_or_java"] == "python" else False
            java = True if request.POST["python_or_java"] == "java" else False
            code_config = request.FILES["code_config"]
            code_file_java_task_1 = request.FILES["code_file_java_task_1"]
            code_file_java_task_2 = request.FILES["code_file_java_task_2"]
            code_file_java_task_3 = request.FILES["code_file_java_task_3"]
            code_file_python_map_1 = request.FILES["code_file_python_map_1"]
            code_file_python_map_2 = request.FILES["code_file_python_map_2"]
            code_file_python_map_3 = request.FILES["code_file_python_map_3"]
            code_file_python_reduce_1 = request.FILES["code_file_python_reduce_1"]
            code_file_python_reduce_2 = request.FILES["code_file_python_reduce_2"]
            code_file_python_reduce_3 = request.FILES["code_file_python_reduce_3"]
        except IndexError as e:
            return HttpResponse("Unable to accept submission. Enter valid details")
        try:
            team = Team.objects.get(team_name=t_name)
        except ObjectDoesNotExist as e:
            return HttpResponse("Team doesnt exist. Enter the team name submitted in the project form.")
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
            return HttpResponse("Cannot accept this submission. Please submit valid data")
        try:
            run.apply_async([submission.id])
        except Exception as e:
            return HttpResponse("Cannot add submission to queue. Contact any TA for resolution")
        return HttpResponse(
            "Done. You will receive your results via an email. The submission id is " + submission.id
            + ". Use this submission id to get in touch in case you dont get an email result.")
