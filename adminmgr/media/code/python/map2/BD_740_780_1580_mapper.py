#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)


for line in infile:
	line = line.strip()		#removing the spaces
	my_list = line.split(',')	#splitting the csv file over commas
	
	if(my_list[0]=='ball'):
                runs = my_list[7]
		extras = my_list[8]
		
		batsman = my_list[4]
		bowler = my_list[6]
		
		#1 for deliveries,to add extrs to runs
		print('%s,%s,%s,%s' % (batsman,bowler,'1',int(runs)+int(extras)))

