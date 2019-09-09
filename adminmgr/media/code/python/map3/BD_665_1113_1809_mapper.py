#!/usr/bin/python3
import sys
import csv
infile = sys.stdin

for line in infile:
	line = line.strip()
	my_list = line.split(',')
	#print(my_list)
	if(my_list[0]=='info' and my_list[1]=='venue'):
		venue=my_list[2:]
		s=','
		ven=s.join(venue)
		#if(ven[0]=="'" or ven[0]=='"'):
		#ven=ven[1:len(ven)-1]
		
	if(my_list[0]=='ball'):
		#print('inside ball')
		batsman=my_list[4]
		runs=my_list[7]
		runs=int(runs)
		#print(runs)
		extra=my_list[8]
		extra=int(extra)
		if(extra==0):
			print('%s_%s_%s_%s' % (ven,batsman,runs,'1',))
		else:
			continue
			
#print('%s\t%s\t%s\t%s' % (batsman,bowler,'1',int(run)+int(extra)))
