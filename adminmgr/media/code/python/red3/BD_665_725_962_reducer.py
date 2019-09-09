#!/usr/bin/python3
import csv
import sys

Dict ={}
inter_dict ={}
final_dict ={}

for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	venue,batsman,runs,deli = line_val[0], line_val[1], line_val[2], line_val[3]
	try:
		count_runs  = int(runs)
		count_deli = int(deli)
	
	
	except ValueError:
		continue
	key = venue,batsman
	if key in Dict:
		Dict[key][0] += count_runs
		Dict[key][1] += count_deli
	else:
		Dict[key] = [count_runs,count_deli]
		
for key in Dict:
	if(Dict[key][1] >= 10):
		inter_dict[key] = [(100*Dict[key][0]/Dict[key][1]) , Dict[key][0]]


for i in sorted(inter_dict.items() , key = lambda x :(x[0][0],x[1][0],x[1][1]) ):

	key = i[0][0]
	batsman = i[0][1]
	runs = i[1][1]
	runrate = i[1][0]
	if key in final_dict:
		if(runrate > final_dict[key][0] or (runrate == final_dict[key][0] and runs > final_dict[key][1])):
			final_dict[key][0] = runrate
			final_dict[key][1] = runs
			final_dict[key][2] = batsman
			
	else:
		final_dict[key] = [runrate,runs,batsman]
	
		

for iterator in final_dict:
	print("%s,%s" % (iterator,final_dict[iterator][2]))
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

		

