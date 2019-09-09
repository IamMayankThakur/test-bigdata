#!/usr/bin/python
import sys
import csv
infile = sys.stdin
for line in infile:
	line = line.strip()
	cols = line.split(',')
	if(mcols[0] == 'ball'):
		out = 0
		if(cols[9] in ["lbw","caught","caught and bowled","bowled","stumped","hit wicket"]):
			out = 1	
		key_list = cols[4]+','+cols[6]+','+str(out)
		print('%s\t%s' % (key_list,'1'))  
