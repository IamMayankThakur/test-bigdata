#!/usr/bin/python3
import sys
import csv
infile = sys.stdin

for line in infile:
	line=line.strip()
	my_list = line.split(',')
	#print(my_list)    
	if(my_list[0]=='ball'):
		#print(my_list[9])
		out = my_list[9]            
		if(out == '""' or out == 'run out' or out == 'retired hurt'):
			out_count=0
		else:
			out_count=1
		print(my_list[4],',',my_list[6],',',str(out_count),',','1',sep="")		
		#print("%s,%s,%s,%s" % (my_list[4],my_list[6],str(out_count),'1')  
