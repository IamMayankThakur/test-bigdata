#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)
val=0
venue=""
#fuel column index 8
for line in infile:
	line = line.strip()
    
	my_list = line.split(',')	
	if(my_list[1]=='venue' and len(my_list)<9 ):
			if(len(my_list)>3):
				venue=str(my_list[2])+','+str(my_list[3])
			else:
				venue=my_list[2]
			
			
	
			
	if(len(my_list)>=9):
		if(int(my_list[8])==0):
		
		
		
			key=str(my_list[4])+';'+venue
		
			val=int(my_list[7])
			print('%s\t%s' % (key,str(val)))
		
			

			
		
			
		
		
	
		

			
		
		
		
		
