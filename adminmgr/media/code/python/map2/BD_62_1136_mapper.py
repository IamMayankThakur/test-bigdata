#!/usr/bin/python3
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
		print(my_list[4],",",my_list[6],",",1,",",my_list[9],",",my_list[7],",",my_list[8])
	#print(lineno)	
	#print("count",count)
