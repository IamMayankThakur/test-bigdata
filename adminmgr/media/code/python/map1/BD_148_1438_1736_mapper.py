#!/usr/bin/python3
import sys
import csv

infile=sys.stdin

for line in infile:
	line=line.strip()
	info=line.split(',')
	if(info[0]=="ball"):
		if(info[9]=='""' or info[9]=="run out" or info[9]=="retired hurt"):
			print("%s,%s\t%s\t%s" % (info[4],info[6],0,1))
		else:
			print("%s,%s\t%s\t%s" %(info[4],info[6],1,1))

