#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import itertools


pairs = dict()

for line in sys.stdin:
    line = line.strip()
    values = line.split(',')

    bowler, batsman, runs, delivs = values[0], values[1], int(values[2]), int(values[3])

    if((bowler, batsman) in pairs.keys()):
        ball = pairs[bowler, batsman] #dict that contains #runs, #delivs
        pairs[bowler, batsman] = (runs + ball[0], delivs + ball[1]) #update both fields
    else:
        pairs[bowler, batsman] = (runs, delivs) #initialize new pair

for keys, vals in list(pairs.items()):
    v1,v2 = vals[0],vals[1]
    if(vals[1] < 6): ##delivs for any pair less than 5, remove
        del pairs[keys]

old = []
for i in sorted(pairs.items(), reverse = True, key = lambda x: x[1]): #sort pairs on #delivs descending
     old.append(i)

i = 0
j = 0
for k in range(len(old) - 1):
    if(old[k][1][0] == old[k+1][1][0]):
        pass
    elif(old[k][1][0] != old[k+1][1][0]):
        old[i:k+1] = sorted(old[i:k+1], reverse = False, key = lambda x: x[1][1])
        i = k + 1
    new_i  = i
old[new_i:k+2] = sorted(old[new_i:k+2],reverse = False,key =  lambda x:x[1][1])

for i in old:
    print("%s,%s,%d,%d"%(i[0][0],i[0][1],i[1][0],i[1][1]))
        
