#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)

for line in infile:
    line = line.strip()
    line_list = line.split(',')
    # print(line_list)
    if(line_list[0]!="ball") :
    		continue
    print("%s,%s\t%s\t1" % (line_list[6],line_list[4],int(line_list[7])+int(line_list[8])))