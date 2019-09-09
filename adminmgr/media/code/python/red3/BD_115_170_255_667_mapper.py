#!/usr/bin/env python3
import sys
import csv

bats = dict()
global venue
venue = ""
first = 0
for line in sys.stdin:
	line = line.strip()
	attr = line.split(',')
	#print(attr)
	if(first == 0):
		first = 1
		continue
	if(attr[0]=='info' and attr[1]=='venue'):
		if len(attr) > 3:			
			venue = attr[2]+","+attr[3]
		else:
			venue = attr[2]
		#print(bats)
		bats.clear()
		#print(venue)
	if(attr[0]=='version'):
		strike = 0
		score = 0
		batsman = ""
		ball = 0
		for x in bats.keys():
			bats[x]= [(int(bats[x][0])*100)/int(bats[x][1]),int(bats[x][0]),int(bats[x][1])]
		#print(bats)
		for x in bats.keys():
			if bats[x][0] > strike and bats[x][2]>9:
				strike = bats[x][0]
				score = bats[x][1]
				ball = bats[x][2]
				batsman = x
			if bats[x][0] == strike  and bats[x][2]>9:
				if bats[x][1] > score:
					strike = bats[x][0]
					score = bats[x][1]
					ball = bats[x][2]
					batsman = x		
		print(venue+'\t'+batsman+'\t'+str(strike)+'\t'+str(score)+'\t'+str(ball))
	if(attr[0]=='ball'):
		if attr[4] not in bats.keys():
			bats[attr[4]] = [attr[7],1]
		else:
			if attr[8] == '0':
				bats[attr[4]][0] = int(bats[attr[4]][0]) + int(attr[7])
				bats[attr[4]][1] = int(bats[attr[4]][1]) + 1

