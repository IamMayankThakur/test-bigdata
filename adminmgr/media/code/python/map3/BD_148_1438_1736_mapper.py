#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
venue = ""
for line in infile:
	line = line.strip()
	info = line.split(',')
	if(info[1] == 'venue'):
		try:
			venue=info[2]+","+info[3] #if venue contains a comma
		except:
			venue=info[2] #if venue does not contain a comma
	elif(info[0] == "ball"):
		if(int(info[8])==0):
			batsman= info[4]
			runs= info[7]
			print('%s\t%s,%s' % (venue,batsman,runs))	
