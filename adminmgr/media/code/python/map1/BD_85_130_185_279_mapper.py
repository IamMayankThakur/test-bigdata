#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
for line in infile:
    line = line.strip()
    ll = line.split(',')
    if(ll[0]!="ball") :
    		continue
    if(ll[9]=='""') or (ll[9]=="run out") or (ll[9]=="retired hurt"):
    	print("%s,%s\t%s\t1" % (ll[4],ll[6],0))
    else:
    	print("%s,%s\t%s\t1" % (ll[4],ll[6],1))

