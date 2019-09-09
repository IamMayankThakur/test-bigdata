#!/usr/bin/python3
import sys
from operator import itemgetter
import csv

stad_results = {}
for line in sys.stdin:
	line=line.strip()
	temp=line.split('=')
	stad_name=temp[0]
	bat_pair = temp[1]
	bat_temp = bat_pair.split(',')
	bat_name = bat_temp[0]
	runs = bat_temp[1]
	if stad_name in stad_results:
		if bat_name not in stad_results[stad_name]:
			stad_results[stad_name][bat_name]=[0, 1, 0]
			stad_results[stad_name][bat_name][0]+=int(runs)
		else:
			stad_results[stad_name][bat_name][1]+=1
			stad_results[stad_name][bat_name][0]+=int(runs)
	else:
		stad_results[stad_name]={}
		stad_results[stad_name][bat_name]=[0, 1, 0] #number of runs, no of balls, strike rate
		stad_results[stad_name][bat_name][0]+=int(runs)



for stad_name in list(stad_results):
	for curr_bat in list(stad_results[stad_name]):
		if stad_results[stad_name][curr_bat][1] < 10:
			del stad_results[stad_name][curr_bat]


for stad_name in stad_results:
	for curr_bat in stad_results[stad_name]:
		strike = (stad_results[stad_name][curr_bat][0]*100)/stad_results[stad_name][curr_bat][1]
		stad_results[stad_name][curr_bat][2] = strike
		

fin={}
for stad_name in stad_results:
	max_strike=0
	for curr_bat in stad_results[stad_name]:
		if(stad_results[stad_name][curr_bat][2]>=max_strike):
			if(stad_results[stad_name][curr_bat][2]==max_strike):
				recent=fin[stad_name]
				if(stad_results[stad_name][recent][0]<stad_results[stad_name][curr_bat][0]):

					max_strike=stad_results[stad_name][curr_bat][2]
					fin[stad_name]=curr_bat
	
			else:	
				max_strike=stad_results[stad_name][curr_bat][2]
				fin[stad_name]=curr_bat
Final=sorted(fin.items())
for i in Final:
	print(i[0]+","+str(i[1]))




