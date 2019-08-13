from django.http import HttpResponse
from django.shortcuts import render
from django.views import View

from .models import Submission
from .models import Team


class CodeUpload(View):
    def get(self, request):
        return render(request, 'index.html')

    def post(self, request):
        print(request.POST)
        print(request.FILES)
        email = request.POST["email"]
        code_file = request.FILES["code_file"]
        assign_no = request.POST['assignno']
        team = Team.objects.get(member_1=email)
        # TODO: Check email for all members not only 1
        submission = Submission(team=team, assignment_no=assign_no, code_file=code_file)
        submission.save()
        return HttpResponse("Done. You will receive your results via an email")
