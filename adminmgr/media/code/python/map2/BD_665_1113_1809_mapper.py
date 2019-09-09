#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)

#fuel column index 8
for line in infile:
	line = line.strip()
	my_list = line.split(',')
	#print(my_list)
	if(my_list[0]=='ball'):
		out=''
		bowler=my_list[6]
		batsman=my_list[4]
		run=my_list[7]
		extra=my_list[8]
		print('%s,%s\t%s\t%s' % (bowler,batsman,int(run)+int(extra),1))

