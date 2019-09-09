#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
for line in infile:
    line = line.strip()
    ll = line.split(',')
    if(ll[0]!="ball") :
    		continue
    print("%s,%s\t%s\t1" % (ll[6],ll[4],int(ll[7])+int(ll[8])))
