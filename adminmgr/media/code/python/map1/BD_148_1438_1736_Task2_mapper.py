#!/usr/bin/python3

import sys
import csv

infile = sys.stdin
for line in infile:
    line = line.strip()
    my_list = line.split(',')
    if(len(my_list)< 10):
    	continue
    elif(my_list[9] == 'run out' or my_list[9] == 'retired hurt' or my_list[9] == '""'):
        print('%s,%s,%d,%d' % (my_list[4],my_list[6],0,1))
    else:
        print('%s,%s,%d,%d' % (my_list[4],my_list[6],1,1))
