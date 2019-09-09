#!/usr/bin/python3

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
	line = line.strip()
	line = line.split(",")
	if line[0] == "ball":
		batsman = line[4]
		bowler = line[6]
		runs = int(line[7]) + int(line[8])
		#extra = line[8]
		pairs=[]
		#pairs.append([batsman + " " + bowler,runs])
		print(bowler+','+batsman+':'+str(runs)) 

