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
        lol[k][1]+=v[1]
    else:
        lol[k] = [v[0],v[1]]

l1 = []
for x in lol:
    if(lol[x][1]>=10):
        if(len(x.split(","))>2):
            l1.append([x.split(",")[0]+","+x.split(",")[1],x.split(",")[2],((lol[x][0]*100.00)/lol[x][1])])
        else:
            l1.append([x.split(",")[0],x.split(",")[1],((lol[x][0]*100.00)/lol[x][1])])
#print(l1)
l2 = dict()
for i in l1:
    if i[0] in l2.keys():
        if(l2[i[0]][1]<i[2]):
            l2[i[0]] = [i[1],i[2]]
    else:
        l2[i[0]] = [i[1],i[2]]

l3 = [[i,l2[i][0],l2[i][1]] for i in l2]
l3.sort(key = lambda x:x[0])
for i in l3:
        print('%s,%s' % (i[0],i[1]))
    




