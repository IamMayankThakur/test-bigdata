#!/usr/bin/python3

import sys

dictionary = dict()
final_list = []
for line in sys.stdin:
	line = line.strip().split('|')
	try:
		runs = int(line[0])
	except ValueError:
		continue
	batsman_bowler = (line[1],line[2])
	if batsman_bowler not in dictionary:
		dictionary[batsman_bowler] = [0,0]
	dictionary[batsman_bowler][0] += runs
	dictionary[batsman_bowler][1] += 1

for batsman_bowler in dictionary:
	if(dictionary[batsman_bowler][1]>5):
		final_list.append([batsman_bowler[1],batsman_bowler[0],dictionary[batsman_bowler][0],dictionary[batsman_bowler][1]])

final_list = sorted(final_list,key=lambda x: (-x[2],x[3],x[0],x[1]))
for item in final_list:
	print(item[0],item[1],item[2],item[3],sep = ",",end = "\t\n")
