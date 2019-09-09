#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
record = {}
batter = None

for line in infile:
	line = line.strip()
	line = line.split("\t")
	strike=float(line[2])
	venue = line[0]
	if venue not in record:
		record[venue] = []
		mxsr = 0
	elif(strike>mxsr):
		batter = line[1]
		record[venue] = [batter]
		mxsr = strike
	elif(strike == mxsr):
		batter = line[1]
		record[venue].append(batter)

venue = sorted(list(record.keys()))

for x in venue:
	print('%s,%s' % (x,record[x][0]))
