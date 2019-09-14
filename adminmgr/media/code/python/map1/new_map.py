#!/usr/bin/python3 BD_62_1136_mapper.py
import sys
import csv
infile = sys.stdin
#next(infile)

#fuel column index 8
count=0
for line in infile:
	line = line.strip()
	my_list = line.split(',')
	#print(my_list)
	my_list1=my_list[0]
	#print("from line read ",my_list[0])
	#lineno+=1
	if my_list[0]=='ball':
		count+=1
		bat=my_list[4]
		bow=my_list[6]
		out = my_list[9]
		w=0            
		if(out != '""' and out != 'run out' and out != 'retired hurt'):
			w=1
		else:
			w=0
		print(bat,bow,w,'1',sep=",")
	#print(lineno)	
	#print("count",count)
