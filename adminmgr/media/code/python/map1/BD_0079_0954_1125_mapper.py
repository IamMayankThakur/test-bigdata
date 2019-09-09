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
    # print("(%s,%s,%s)\t1" % (line_list[4],line_list[6],line_list[9]))
    if(line_list[9]=='""') or (line_list[9]=="run out") or (line_list[9]=="retired hurt"):
    	print("%s,%s\t%s\t1" % (line_list[4],line_list[6],0))
    else:
    	# print(len(line_list[9]))
    	print("%s,%s\t%s\t1" % (line_list[4],line_list[6],1))

