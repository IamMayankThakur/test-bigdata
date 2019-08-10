from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_protect


@csrf_protect
def code_upload(request):
    if request.method == 'POST':
        print(request)
        print(request.FILES)
        print(request.path)
        print(request.POST)
        return HttpResponse("Done")
    else:
        return render(request, 'index.html')