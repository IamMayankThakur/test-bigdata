#!/usr/bin/python3
import sys
import csv
i = 0
infile = sys.stdin
for line in infile: #splits into rows
    line = line.strip() 
    my_list = line.split(',') #splits row into params
    i = i+1

    #print(my_list)
    #if(i == 50):
    #	break
    #print((my_list))
    if(len(my_list)< 7): 
    	 continue
    else:
         j=int(my_list[8])+int(my_list[7])
         print('%s,%s,%d,%d' % (my_list[6],my_list[4],j,1))
