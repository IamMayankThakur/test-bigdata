#!/usr/bin/python3
import sys
import csv
infile = sys.stdin


for line in infile:
        line = line.strip()
        my_list = line.split(',')
        if(len(my_list)>10):
                if(len(my_list[10])>2 and not(my_list[9]=='run out' or my_list[9]=='retired hurt')):
                        print('%s,%s' % (my_list[10]+"|"+my_list[6],'1;1'))
                else:
                        print('%s,%s' % (my_list[4]+"|"+my_list[6],'0;1'))


