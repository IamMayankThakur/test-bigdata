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
		out=my_list[9]  
		if(out=='""' or out=='run out' or out=='retired hurt'):
			outt=0
		else:
			outt=1
		#outt=str(outt)
		print('%s,%s\t%s\t%s' % (bowler,batsman,outt,1))
		#print(bowler,batsman,outt,1,sep="\t")
