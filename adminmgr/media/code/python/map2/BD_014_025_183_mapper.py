#!/usr/bin/python3
import csv
import sys

batsman = ""
bowler = ""
runs = None
reader = []

myfile = sys.stdin

for line in myfile:
	line = line.strip()
	row = line.split(',')
	
	if(row[0] == "ball"):
		batsman = row[4]
		bowler = row[6]
		runs = int(row[7]) + int(row[8])
		print("%s,%s,%d" % (batsman, bowler, runs))
