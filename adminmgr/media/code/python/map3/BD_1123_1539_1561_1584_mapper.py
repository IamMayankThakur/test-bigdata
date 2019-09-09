#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)


current_venue = ''
previous_venue = ''
for line in infile:
	line = line.strip()
	l = line.split(',')
	if(l[1]=='venue'):
		if(l[2][0] == '"'):
			current_venue = l[2]+','+l[3]
		else:
			current_venue = l[2]
	if(l[0]=='ball' and l[8]=='0'):
		runs =current_venue+','+l[4]+','+str(l[7])
		print(runs," 1")
