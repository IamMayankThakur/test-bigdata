#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)

stadium=""
one="1"
for line in infile:
	line = line.strip()
	my_list = line.split(',')
	if(my_list[1] == "venue"):
		stadium = my_list[2]
		if(len(my_list) > 3):
			stadium = stadium + "," + my_list[3]
		stadium = stadium.strip()
	if((my_list[0] == "ball") and (my_list[8] == "0")):
		print('%s,%s' % (stadium + ',' + my_list[4], my_list[7] + ',' + one))
    		
    
