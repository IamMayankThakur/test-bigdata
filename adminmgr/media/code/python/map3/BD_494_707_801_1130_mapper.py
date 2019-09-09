#!/usr/bin/python3
import sys
import csv

venue=''
for line in sys.stdin:
	line=line.strip()
	curr_line=line.split(',')
	temp=len(curr_line)
	if(curr_line[0]=='info' and curr_line[1]=='venue'):
		if(temp>=4):
			venue=curr_line[2]+','+curr_line[3]
			#venue=curr_line[-2]+','+curr_line[-1] in case more than 4 items in venue but didnt find such an instance in data set
		else:
			venue=curr_line[2]

	elif(curr_line[0]=='ball'):
		ball_run=int(curr_line[8])
		if(ball_run==0):
			curr_venue = venue
			temp1 = curr_line[4]+','+curr_line[7]
			print(curr_venue+'='+temp1)
		else:
			continue

