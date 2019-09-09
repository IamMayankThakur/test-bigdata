#!/usr/bin/python3
#"""mapper.py"""

import sys

out = 1
dictmapper={}

# input comes from STDIN (standard input)
for line in sys.stdin:
	line = line.strip()
	line = line.split(",")

	if line[0] == "ball":
		batsman = line[4]
		bowler = line[6]
		runs = line[7]
		extra = line[8]
		key = batsman+','+bowler
		print('%s,%s,%s' % (key,runs,extra))
		#print('%s,%s,%s' % (batsman,bowler,runs))
