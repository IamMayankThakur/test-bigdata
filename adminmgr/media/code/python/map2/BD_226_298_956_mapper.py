#!/usr/bin/python3
import sys
import csv
infile = sys.stdin


for line in infile:
        line = line.strip()
        my_list = line.split(',')
        if(len(my_list)>10):
                print('%s,%s' % (my_list[6]+"|"+my_list[4],str(int(my_list[7]) + int(my_list[8]))+';1'))
