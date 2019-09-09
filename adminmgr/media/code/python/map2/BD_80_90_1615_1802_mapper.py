#!/usr/bin/python3
import sys
import csv
infile=sys.stdin
bowler=""
batsman=""
for line in infile:
	line = line.strip()
	line = line.split(",")
	runs=0
	if (line[0]=="ball"):
		bowler =line[6]
		batsman =line[4]
		runs=int(line[7])+int(line[8])
		print(batsman,bowler,runs,1,sep='\t')