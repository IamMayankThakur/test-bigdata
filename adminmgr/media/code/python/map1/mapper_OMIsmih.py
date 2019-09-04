#!/usr/bin/python
import sys
import csv
infile = sys.stdin
for line in infile:
    line = line.strip()
    my_list = line.split(';')
    if(len(my_list) >= 2):
        print("%s\t%s" % (my_list[0],my_list[1]))
    '''if(len(my_list) >= 5):
        key = my_list[1]
        fuel = my_list[8]
        value = "0"
        if(fuel == 'gas'):
            value = "1"
        print('%s\t%s' % (key,value))'''    
