#!/usr/bin/python3
import sys
from operator import itemgetter
import csv

dict={}

for line in sys.stdin:
    line = line.strip()
    key,runs1,extras1=line.split('    ',2)
    runs=int(runs1)
    extras=int(extras1)
    if key not in dict:
        dict[(key)]=[runs+extras,1]
    else:
        dict[(key)][0]+=runs+extras
        dict[(key)][1]+=1

for i in sorted(sorted(dict.keys()),key=lambda k:(-dict[k][0],dict[k][1],k[0])):
    if dict[i][1]>5:
        print(i,dict[i][0],dict[i][1],sep=",")
