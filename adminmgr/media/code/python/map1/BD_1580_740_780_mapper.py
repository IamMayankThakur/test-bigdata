#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)


for line in infile:
	line = line.strip()
	my_list = line.split(',') #to split input into comma sep values
	
	if(my_list[0]=='ball'):
		out=''
		batsman=my_list[4]
		bowler=my_list[6]
		
		out=my_list[9]  
		if(out=='""' or out=='retired hurt' or out=='run out'):
			if_out=0  #not to consider as out for these reasons
		else:
			if_out=1
		#input to the reducer
		print('%s,%s\t%s\t%s' % (bowler,batsman,outt,1))
		