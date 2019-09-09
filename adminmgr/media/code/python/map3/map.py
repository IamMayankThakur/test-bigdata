#!/usr/bin/python3
import sys
import csv
file = sys.stdin
curr_venue=""
for line in file:
	line = line.strip()
	my_list  = line.split(',')
	Venue = my_list[1]
	try:
		if (Venue == 'venue'):
			curr_venue = my_list[2]

		Batsman = my_list[4]
		Runs = int(my_list[7])

		if(curr_venue != ""):
			print('%s\t%s\t%d\t%d'%(curr_venue,Batsman,Runs,1))	
	except:
		continue
