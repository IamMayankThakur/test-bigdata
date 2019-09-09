#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)

#fuel column index 8
for line in infile:
    line = line.strip()
    l = line.split(',')
    if(l[0]=='ball'):
        #if(l[9]=='lbw' or l[9]=='bowled' or l[9]=='caught' or l[9]=='caught and bowled' or l[9]=='stumped' or l[9]=='hit wicket' or l[9]=='obstructing the field'):
        runs =l[4]+','+l[6]+','+str(l[7])+','+str(l[8])
        print(runs," 1")
        '''else:
            not_out=l[4]+','+l[6]+','+"0"
            print(not_out," 1")'''
    	

