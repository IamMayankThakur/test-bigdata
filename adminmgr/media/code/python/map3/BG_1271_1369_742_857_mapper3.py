#!/usr/bin/python3
import sys
import csv
inf = sys.stdin
record = {}
venue = None
#record[venue][batsman]:[runs,balls]
for line in inf:
	line = line.strip()
	line = line.split(",")
	if(line[1] == 'venue'):
		if(len(line) == 4):
			venue = line[2]+','+line[3]
		else:
			venue = line[2]
		if(venue not in record):
			record[venue] = {}
	elif(line[0] == 'ball'):
		batter = line[4]
		if batter not in record[venue]:
			record[venue][batter] = [0,0]
		runs = int(line[7])
		record[venue][batter][0] += runs
		if(int(line[8]) == 0 or (int(line[8]) == 1 and runs>0)):
			record[venue][batter][1] += 1

for x in record:
	for b in record[x]:
		if(record[x][b][1] >= 10):
			strike = record[x][b][0]/record[x][b][1]
			print('{}\t{}\t{}'.format(x,b,strike))
