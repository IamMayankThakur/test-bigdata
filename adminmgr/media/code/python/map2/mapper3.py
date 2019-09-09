#!/usr/bin/python
import sys
import csv
infile = sys.stdin
#next(infile)

#fuel column index 8
for line in infile:
	line = line.strip()		#removing the spaces
	my_list = line.split(',')	#splitting the csv file over commas
	#print(my_list)
	if(my_list[0]=='ball'):
		#out=''
		batsman = my_list[4]
		bowler = my_list[6]
		runs = my_list[7]
		extras = my_list[8]
		print('%s,%s,%s,%s' % (batsman,bowler,int(runs)+int(extras), '1'))


