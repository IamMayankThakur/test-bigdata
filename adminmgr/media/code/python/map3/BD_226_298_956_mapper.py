#!/usr/bin/python3
import sys
import csv
infile = sys.stdin

venue=""
for line in infile:
        line = line.strip()
        my_list = line.split(',')
        if(my_list[1]=='venue'):
                if(len(my_list)>3):
                        venue = my_list[2]+","+my_list[3]
                else:
                        venue = my_list[2]
        else:
                if(len(my_list)>4 and my_list[8]=='0'):
                        print("%s@%s"%(str(venue)+"|"+str(my_list[4]),my_list[7]+";1")) 
