#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
D = {}
current_bowler = ""
current_batsman = ""
current_wic = 0
current_deli = 0
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
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
		if (current_batsman != "" ):
			if(current_deli > 5):			
				D[current_batsman,current_bowler] = current_wic,current_deli
		current_wic = count_wic
		current_deli = count_deli
		current_bowler = bowler
		current_batsman = batsman
		
if(current_bowler == bowler and current_batsman == batsman):
	if(current_deli > 5):
	 	D[current_batsman,current_bowler] = current_wic,current_deli

for final in sorted(D.items() , key = lambda s :( -s[1][0],s[1][1]) ):
	print("%s,%s,%d,%d" % (final[0][0],final[0][1],final[1][0],final[1][1]))
