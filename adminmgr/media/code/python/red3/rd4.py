#!/usr/bin/python3
import csv
import sys

current_venue = ""
current_batsman = ""
current_run = 0
current_deli = 0

Dict = {}
for line in sys.stdin:
	line = line.strip()
	line_ = line.split("\t")
	venue,batsman,run,deli = line_[0], line_[1], line_[2], line_[3]
	
	try:
		count_run  = int(run)
		count_deli = int(deli)
	
	
	except ValueError:
		continue
		
	key = venue,batsman
	if key in Dict:
		Dict[key][0] += count_run
		Dict[key][1] += count_deli
	else: 
		Dict[key] = [count_run,count_deli]

my_list = []

for item in Dict.items():
    my_list.append([item[0][0], item[0][1], item[1][0], item[1][1], (item[1][0]/float(item[1][1]) * 100)])


top_list =[]
for j in sorted(my_list , key = lambda x :(x[0],-x[4],-x[2],x[1]) ):
  	if((j[0] not in ([val[0] for val in top_list])) and j[3] >= 10):
        	top_list.append(j)

for j in top_list:
    print('%s,%s' % (j[0], j[1]))


