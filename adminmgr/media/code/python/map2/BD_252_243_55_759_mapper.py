#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
for line in infile:
    line = line.strip()
    temp = line.split(',')
    
    if(temp[0]=="ball") :
	continue
	if((temp[9]=="caught") or temp[9]==("bowled")or temp[9]==("lbw") or temp[9]==("caught and bowled") or temp[9]==("hit wicket") or temp[9]==("obstructing the field") or temp[9]==("stumped"))
		continue
    		print("%s,%s\t%s\t1" % (temp[4],temp[6],1))
    else:
    	
		print("%s,%s\t%s\t1" % (temp[4],temp[6],0))
