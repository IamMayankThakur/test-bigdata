#!/usr/bin/env python3
import sys
import csv

for line in sys.stdin:
	line = line.strip()
	details = line.split(',')
	if details[0]=='ball':
		runs = int(details[7]) + int(details[8])
		print('%s,%s\t%s\t%s' % (details[6],details[4],runs,'1'))
		
	
