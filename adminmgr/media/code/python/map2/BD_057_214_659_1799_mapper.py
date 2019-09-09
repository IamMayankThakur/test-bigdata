#!/usr/bin/python3
import sys
import csv
i=0
file=sys.stdin
for val in file:                               #row splitter
    val=val.strip() 
    new_val=val.split(',')                     #rows are now split into parameters
    i=i+1
    if(len(new_val)<7): 
    	continue
    else:
        a=int(new_val[8])
        b=int(new_val[7])
        j=a+b
        print('%s,%s,%d,%d'%(new_val[6],new_val[4],j,1))