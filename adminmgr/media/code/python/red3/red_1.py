#!/usr/bin/python3
from operator import itemgetter
import sys
import csv

current_count_delivery = 0
current_count_runs = 0
current_key_batsman = ""
current_key_venue = ""
Dict = {}
batsman = ""
venue = ""
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	venue,batsman,runs,delivery = line_val[0],line_val[1],line_val[2],line_val[3]
	try:
		count_ball=int(delivery)
		count_runs=int(runs)
	except ValueError:
		continue
   
	if (current_key_batsman == batsman and current_key_venue==venue):
		current_count_delivery += count_ball
		current_count_runs += count_runs 
	else:
		if (current_key_batsman != ""):
			if(current_count_delivery >=10):
				Dict[current_key_venue,current_key_batsman]=current_count_runs,current_count_delivery
		current_count_runs = count_runs
		current_count_delivery = count_ball
		current_key_batsman = batsman
		current_key_venue = venue
		
if (current_key_batsman == batsman and current_key_venue==venue):
	if(current_count_delivery >= 10):
		Dict[current_key_venue,current_key_batsman]=current_count_runs,current_count_delivery
		

lst = sorted(Dict.items(), key=itemgetter(1),reverse = True)
for t in lst:
	print('%s,%s,%d,%d'%(t[0][0],t[0][1],t[1][0],t[1][1]))
	

