#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)
val=0
#fuel column index 8
for line in infile:
	line = line.strip()
    
	my_list = line.split(',')
	if(len(my_list)>=9):
		
		key=str(my_list[4])+','+str(my_list[6])
		val=int(my_list[7])+int(my_list[8])
	
		print('%s\t%s' % (key,str(val)))

			
		
			
		
		
	
		

			
		
		
		
		
