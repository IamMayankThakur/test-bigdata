#!/usr/bin/python3
import sys
import csv
infile = sys.stdin

for line in infile:
    line = line.strip()
    my_list = line.split(',')
    if my_list[0] != "ball":
        continue
    bowler = my_list[6]
    batsman = my_list[4]
    runs_conceeded = int(my_list[7]) + int(my_list[8])
    print('%s,%s\t%s\t%s' % (bowler,batsman,runs_conceeded,'1'))     
