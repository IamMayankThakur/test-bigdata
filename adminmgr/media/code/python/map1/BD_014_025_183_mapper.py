#!/usr/bin/python3
import csv
import sys

batsman = ""
bowler = ""
wicket = None
balls = None
reader = []

myfile = sys.stdin

for line in myfile:
	line = line.strip()
	row = line.split(',')
	
	if(row[0] == "ball"):
		if(row[4] == row[10] and not (row[9] == "retired hurt") and not (row[9] == "run out")):
			batsman = row[4]
			bowler = row[6]
			wicket = 1
			balls = 1
			print("%s,%s,%d,%d" % (batsman, bowler, wicket,balls))
		else:
			batsman = row[4]
			bowler = row[6]
			wicket = 0
			balls = 1
			print("%s,%s,%d,%d" % (batsman, bowler, wicket,balls))
