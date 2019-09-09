#!/usr/bin/python3
import sys
import csv
infile = sys.stdin

for line in infile:
    line = line.strip()
    my_list = line.split(',')
    if my_list[0] != "ball":
        continue
    batsman = my_list[4]
    bowler = my_list[6]
    if my_list[9] == 'run out' or my_list[9] == '""' or my_list[9] == "retired hurt":
        dismissed = '0'
    else:
        dismissed = '1'
    print('%s,%s\t%s\t%s' % (batsman,bowler,dismissed,'1'))     
