#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)

#fuel column index 8
for line in infile:
   line = line.strip()
    
   my_list = line.split(',')
   if(len(my_list)>=9):
      val=0
      if(my_list[9]!='run out' and my_list[9]!='\"\"'):
		val=1
			
	elif(my_list[9]!='run out'):
		val=0
		
	key=str(my_list[4])+','+str(my_list[6])
	
	print('%s\t%s' % (key,str(val)))
			
		
		
		
		
