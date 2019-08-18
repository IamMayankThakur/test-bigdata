from django.http import HttpResponse
from django.shortcuts import render
from django.views import View

from .models import Submission
from .models import Team
from .utils import run


class CodeUpload(View):
    def get(self, request):
        return render(request, 'index.html')

    def post(self, request):
        t_name = request.POST["team_name"]
        code_file = request.FILES["code_file"]
        assign_no = request.POST['assignno']
        team = Team.objects.get(team_name=t_name)
        submission = Submission(team=team, assignment_no=assign_no, code_file=code_file)
        submission.save()
        run.apply_async([submission.id])
        return HttpResponse("Done. You will receive your results via an email")
