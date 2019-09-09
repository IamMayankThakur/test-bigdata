#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
for line in infile:
	line = line.strip()
	lines= line.split(',')
	if(lines[0]=='ball'):
		#nature=lines[9]
		#print(nature=='""')
		
		print('%s\t%s\t%s\t%s\t%s' % (lines[4],lines[6],lines[7],lines[8],1))
		

