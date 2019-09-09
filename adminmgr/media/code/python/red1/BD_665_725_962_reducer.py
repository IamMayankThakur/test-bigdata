#!/usr/bin/python3
import csv
import sys

current_bowler = ""
current_batsman = ""
current_wic = 0
current_deli = 0

Dict = {}
for line in sys.stdin:
	line = line.strip()
	line_ = line.split("\t")
	batsman,bowler,wic,deli = line_[0], line_[1], line_[2], line_[3]
	
	try:
		count_wic  = int(wic)
		count_deli = int(deli)
	
	
	except ValueError:
		continue
		
	key = batsman,bowler
	if key in Dict:
		Dict[key][0] += count_wic
		Dict[key][1] += count_deli
	else: 
		Dict[key] = [count_wic,count_deli]
				
for i in sorted(Dict.items() , key = lambda x :(-x[1][0],x[1][1],x[0][0],x[0][1]) ):
	if(i[1][1] > 5):
		print("%s,%s,%d,%d" % (i[0][0],i[0][1],i[1][0],i[1][1]))

	'''if(current_bowler == bowler and current_batsman == batsman):
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
		
	
for i in sorted(Dict.items() , key = lambda x :(-x[1][0],x[1][1]) ):
	print("%s,%s,%d,%d" % (i[0][0],i[0][1],i[1][0],i[1][1]))'''
	


