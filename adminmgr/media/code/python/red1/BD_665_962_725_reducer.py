#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

current_bowler = ""
current_batsman = ""
current_wic = 0
current_deli = 0

Dict = {}
for line in sys.stdin:
	line = line.strip()
	line_val = line.split(",")
	batsman,bowler,wic,deli = line_val[0], line_val[1], line_val[2], line_val[3]
	try:
		count_wic  = int(wic)
		count_deli = int(deli)
	
	
	except ValueError:
		continue
	if(current_bowler == bowler and current_batsman == batsman):
		
		current_wic += count_wic
		current_deli += count_deli
	else:
		if (current_batsman != ""):
			if (current_deli > 5): 
				#print('%s\t%s\t%d\t%d'% (current_batsman,current_bowler,current_wic,current_deli))
				Dict[current_batsman,current_bowler] = current_wic,current_deli
		current_wic = count_wic
		current_deli = count_deli
		current_bowler = bowler
		current_batsman = batsman
		
if(current_bowler == bowler and current_batsman == batsman):
	if(current_deli > 5): 
		#print('%s\t%s\t%d\t%d'% (current_batsman,current_bowler,current_wic,current_deli))
		Dict[current_batsman,current_bowler] = current_wic,current_deli
		
		
for i in sorted(Dict.items() , key = lambda x :(-x[1][0],x[1][1],x[0][0],x[0][1]) ):
	print("%s,%s,%d,%d" % (i[0][0],i[0][1],i[1][0],i[1][1]))
	

