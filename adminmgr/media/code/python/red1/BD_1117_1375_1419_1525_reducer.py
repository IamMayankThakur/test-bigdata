#!/usr/bin/python3

import sys

current_word = None
current_count = [0,0]
lol = dict()
for l in sys.stdin:
    l = l.strip()
    k,v = l.split("\t")
    
    try:
        v = [int(i) for i in v.split(",")]
    except ValueError:
        continue
    if k in lol:
        lol[k][0]+=v[0]
        lol[k][1]+=1
    else:
        lol[k] = [v[0],1]

l1 = [[x,lol[x][0],lol[x][1]] for x in lol]


l1.sort(key=lambda x : x[0])
l1.sort(key=lambda x : x[2])
l1.sort(key=lambda x : x[1], reverse = True)


for i in l1:
    if(i[2]>5):
        print('%s,%s,%s' % (i[0],i[1],i[2]))
    




