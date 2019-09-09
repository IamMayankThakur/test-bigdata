#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
venue=''
for line in infile:
	line=line.strip()
	my_list = line.split(',')
	#print(my_list)
	if(my_list[0]=='info' and my_list[1]=='venue'): 
		if(len(my_list)==4):		
			venue=my_list[2]+','+my_list[3]
		else:
			venue=my_list[2]
		#print(venue) 
	if(my_list[0]=='ball' and my_list[8]=='0'):
		#print(my_list[9])
		print(venue,',',my_list[4],',',my_list[7],',','1',sep="")		
		#print("%s,%s,%s,%s" % (my_list[4],my_list[6],str(out_count),'1')  
