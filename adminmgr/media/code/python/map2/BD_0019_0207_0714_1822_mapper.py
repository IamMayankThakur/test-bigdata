#!/usr/bin/python3
import sys
import csv
i = 0#coded by PES1201701822
fileread = sys.stdin
for line in fileread:
    line = line.strip() 
    my_list = line.split(',')
    if(len(my_list)< 7): 
    	 continue
    else:
         j=int(my_list[8])+int(my_list[7])
         print(my_list[6],my_list[4],j,1,sep="$")
