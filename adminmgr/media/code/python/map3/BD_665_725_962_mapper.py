#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
venue=""
Dict = {}
for line in infile:
    line = line.strip()
    
    if(line.split(",")[0] =="ball"):
    	 my_list = line.split(",")
    	 if(int(my_list[8])==0):
               print('%s\t%s\t%d\t%d' % (venue,my_list[4],int(my_list[7]),1))
               
    elif(line.split(",",2)[0]=="info"):
	       my_list = line.split(",",2)
	       if(my_list[1]=="venue"):
	       		venue = my_list[2]
	       		



