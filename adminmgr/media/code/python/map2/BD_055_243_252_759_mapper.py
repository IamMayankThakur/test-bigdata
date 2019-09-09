#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
for line in infile:
    line = line.strip()
    temp = line.split(',')
    if(temp[0]=="ball") :
    		
    	print("%s,%s\t%s\t1" % (temp[6],temp[4],int(temp[7])+int(temp[8])))
