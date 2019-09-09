#!/usr/bin/python3

import sys

d = {}
for line in sys.stdin:
    words = line.strip().split("|") 
    try:
        w = int(words[0])
    except ValueError:
        continue
    if (words[1],words[2]) not in d:
        d[(words[1],words[2])]=[0,0]
    d[(words[1],words[2])][0]+=w
    d[(words[1],words[2])][1]+=1

l=[]
for i in d:
    if(d[i][1]>5):
        l.append(list(i)+d[i])
l=sorted(l,key=lambda x:(-x[2],x[3],x[0]))
for i in l:
	print(i[0],i[1],i[2],i[3],sep = ",")