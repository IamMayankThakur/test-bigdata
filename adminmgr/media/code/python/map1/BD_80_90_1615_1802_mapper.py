#!/usr/bin/python3
import sys
import csv
infile=sys.stdin
bowler=""
batsman=""
ball=1
out=0
for line in infile:
	line = line.strip()
	line = line.split(",")
	if (line[0]=="ball"):
		bowler = line[6]
		batsman = line[4]
		ball=1
		if((line[9]!="run out") and (line[9]!='""')and (line[9]!="retired hurt")):
			out=1
		else:
			out=0
		print(bowler,batsman,out,1,sep="\t")