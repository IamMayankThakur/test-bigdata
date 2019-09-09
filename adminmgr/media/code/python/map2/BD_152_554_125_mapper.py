#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)

#fuel column index 8
for line in infile:
	line = line.strip()
	record = line.split(',')
	if(record[0] == 'ball'):
		runs = int(record[7]) + int(record[8])
		print("%s,%s,%s,%s" % (record[6], record[4], runs, 1))
