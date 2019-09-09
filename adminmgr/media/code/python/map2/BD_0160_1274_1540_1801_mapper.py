#!/usr/bin/python3
import sys
import csv
infile = sys.stdin


for line in infile:
	line=line.strip()
	my_list = line.split(',')
	extra=0
	#print(my_list)    
	if(my_list[0]=='ball'):
		#print(my_list[9])
		run = int(my_list[7])          
		
		if(my_list[8]!='0'):
			extra=int(my_list[8])
 
		print(my_list[6],',',my_list[4],',',str(run+extra),',','1',sep="")		
		#print("%s,%s,%s,%s" % (my_list[4],my_list[6],str(out_count),'1')  
