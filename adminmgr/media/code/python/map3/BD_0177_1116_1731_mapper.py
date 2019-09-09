#!/usr/bin/python3
import sys
for line in sys.stdin:
	line = line.strip()
	line=line.split(",")
	if(len(line)==3 and line[1]=='venue'):
		venue=line[2]
	if(len(line)==4 and line[1]=='venue'):
		venue=str(line[2])+","+str(line[3])
	if(len(line)>9 and line[8]=='0'):
		print(venue,line[4],line[7],1,sep="_")

