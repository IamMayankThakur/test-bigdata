#!/usr/bin/python3
import sys
import csv
i = 0
infile = sys.stdin
for line in infile:
    line = line.strip() 
    my_list = line.split(',')
    i = i+1
    if(len(my_list)< 7):
    	continue
    elif(my_list[9] == 'run out' or my_list[9] == '""'):
    	print('%s,%s,%d,%d' % (my_list[5],my_list[6],0,1))
    else:
    	 print('%s,%s,%d,%d' % (my_list[5],my_list[6],1,1))