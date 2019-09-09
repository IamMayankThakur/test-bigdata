#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
venue = ""

for line in infile:
    line = line.strip()
    my_list = line.split(',')
    if my_list[0] == "info" and my_list[1] == "venue":
        venue = ",".join(my_list[2:])
        continue
    if my_list[0] != "ball" or my_list[8] != '0':
        continue
    batsman = my_list[4]
    runs_scored = my_list[7]
    print('%s+%s\t%s\t%s' % (venue,batsman,runs_scored,'1'))     
