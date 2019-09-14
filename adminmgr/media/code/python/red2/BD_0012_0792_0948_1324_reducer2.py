#!/usr/bin/python3
import csv
import sys

current_bowler = ""
current_batsman = ""
current_run = 0
current_deli = 0

Dict = {}
for line in sys.stdin:
	line = line.strip()
	line_ = line.split("\t")
	bowler,batsman,run,deli = line_[0], line_[1], line_[2], line_[3]
	
	try:
		count_run  = int(run)
		count_deli = int(deli)
	
	
	except ValueError:
		continue
		
	key = batsman,bowler
	if key in Dict:
		Dict[key][0] += count_run
		Dict[key][1] += count_deli
	else: 
		Dict[key] = [count_run,count_deli]
				
for i in sorted(Dict.items() , key = lambda x :(-x[1][0],x[1][1],x[0][1],x[0][0]) ):
	if(i[1][1] > 5):
		print("%s,%s,%d,%d" % (i[0][1],i[0][0],i[1][0],i[1][1]))

