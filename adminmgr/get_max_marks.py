import csv
from api.models import Team, SubmissionAssignmentOne
from django.db.models import Max


def get_max_marks():
	s1 = list(SubmissionAssignmentOne.objects.values(
		'team__team_name').annotate(Max('score_1')))
	s3 = list(SubmissionAssignmentOne.objects.values(
		'team__team_name').annotate(Max('score_2')))
	s3 = list(SubmissionAssignmentOne.objects.values(
		'team__team_name').annotate(Max('score_3')))
	s4 = list(SubmissionAssignmentOne.objects.values(
		'team__team_name').annotate(Max('score_4')))

	allrows = []
	allrows.append(['Team Name', 'SRN1', 'Viva Marks1', 'SRN2', 'Viva Marks2', 'SRN3', 'Viva Marks3', 'SRN4', 'Viva Marks4', 'Task1 Marks', 'Task2 Marks', 'Task3 Marks', 'Task4 Marks'])
	for i in range(len(s1)):
		row = []
		srn_info = find_row(s1[i]['team__team_name'])
		print(srn_info)
		import pdb; pdb.set_trace()



def find_row(team_name):
	csv_file = csv.reader(open('new_bd_resp.csv', "r"), delimiter=",")
	for row in csv_file:
		if team_name == row[1]:
			return row
