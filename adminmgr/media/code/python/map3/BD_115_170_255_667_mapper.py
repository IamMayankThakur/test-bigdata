#!/usr/bin/env python3
import sys
import csv

bats = dict()
global venue
venue = ""
venues = dict()
first = 0
bats_new = {}
for line in sys.stdin:
	line = line.strip()
	attr = line.split(',')
	if(first == 0):
		first = 1
		continue
	if(attr[0]=='info' and attr[1]=='venue'):
		if len(attr) > 3:			
			venue = attr[2]+","+attr[3]
		else:
			venue = attr[2]
		bats.clear()
		bats_new.clear()
	if(attr[0]=='ball'):
		if attr[4] not in bats.keys():
			bats[attr[4]] = [int(attr[7]),1]
		else:
			if attr[8] == '0':
				bats[attr[4]][0] = int(bats[attr[4]][0]) + int(attr[7])
				bats[attr[4]][1] = int(bats[attr[4]][1]) + 1
		
	if(attr[0]=='version'):
		for x in bats.keys():
			bats_new[x]= [(int(bats[x][0])*100)/int(bats[x][1]),int(bats[x][0]),int(bats[x][1])]
		for x in bats_new.keys():
			if (venue,x) not in venues.keys():
				venues[(venue,x)] = [float(bats_new[x][0]),int(bats_new[x][1]),int(bats_new[x][2])]
			else:
				nsco = venues[(venue,x)][1] + int(bats_new[x][1])
				nball = venues[(venue,x)][2] + int(bats_new[x][2])
				nstk = int(nsco*100)/int(nball) 
				venues[(venue,x)] = [nstk,nsco,nball]

for x in bats.keys():
	bats_new[x]= [(int(bats[x][0])*100)/int(bats[x][1]),int(bats[x][0]),int(bats[x][1])]
for x in bats_new.keys():
	if (venue,x) not in venues.keys():
		venues[(venue,x)] = [float(bats_new[x][0]),int(bats_new[x][1]),int(bats_new[x][2])]
	else:
		nsco = venues[(venue,x)][1] + int(bats_new[x][1])
		nball = venues[(venue,x)][2] + int(bats_new[x][2])
		nstk = int(nsco*100)/int(nball) 
		venues[(venue,x)] = [nstk,nsco,nball]

for value in venues.keys():
	if venues[value][2]>9:
		print(value[0]+'\t'+value[1]+'\t'+str(venues[value][0])+'\t'+str(venues[value][1])+'\t'+str(venues[value][2]))
