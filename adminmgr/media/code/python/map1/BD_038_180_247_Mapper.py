#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
for line in infile:
	line = line.strip()
	lines= line.split(',')
	if(lines[0]=='ball'):
		#print("ok")
		
		nature=lines[9]
		#print(nature=='""')
########### Changed :
		if(nature in ['caught','lbw','bowled','caught and bowled','hit wicket','stumped','obstructing the field']):
			print('%s\t%s\t%d\t%d' % (lines[4],lines[6],1,1))
		else:
			print('%s\t%s\t%d\t%d' % (lines[4],lines[6],1,0))
	

