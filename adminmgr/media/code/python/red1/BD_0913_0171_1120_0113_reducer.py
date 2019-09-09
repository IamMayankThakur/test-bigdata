#!/usr/bin/python3
import sys
import csv

file_contents = sys.stdin
output_dict = dict()

for LINE in file_contents:
	line = LINE.strip()
	LIST = line.split(",")
	batsman_bowler = str(LIST[0]) + "_" + str(LIST[1])
	wickets = int(LIST[2])
	if batsman_bowler in output_dict.keys():				
		wickets_deleveries = output_dict[batsman_bowler]
		wickets_deleveries[0] = wickets_deleveries[0] + wickets		#No. of wickets
		wickets_deleveries[1] = wickets_deleveries[1] + 1		#No. of deleveries
	else:
		output_dict[batsman_bowler] = [wickets,1]


o = list()
for key in output_dict.keys(): 
	if(output_dict[key][1] <= 5):
		continue
	bat_bowl_list = key.split("_")
	wick_del_list = output_dict[key]
	l1 = bat_bowl_list + wick_del_list
	o.append(l1)
o.sort(key = lambda x: (-x[2],x[3],x[0],x[1]))
for l in o:
	print(l[0],l[1],l[2],l[3],sep=",")	

	
		
		
		
