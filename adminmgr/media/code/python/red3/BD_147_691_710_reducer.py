#!/usr/bin/python3
import sys
import itertools
x=dict()
for data in sys.stdin:
    data=data.strip()
    l=data.split("_")
k1=l[0]
k2=l[1]
v1=int(l[2])
v2=int(l[3])
if((k1,k2) in x):
    y=x[k1,k2]
    x[k1,k2]=(v1+y[0], v2+y[1])
else:
    x[k1,k2]=(v1,v2)
prev=[]
for i,j in x.items():
    rr=(j[0]/float(j[1]))
    x[k]=(rr,j[1])
z=dict()
for k in sorted(x.items(),reverse=True,key =lambda x:x[1]):
    old.append(k)
l=[]
l1=[]
rr1=dict()
for a in range(len(prev)):
    if(prev[a][0][0] not in l):
        if(int(old[i][1][1])>9):
            l.append(prev[a][0][0])
            rr1[prev[a][0][0], prev[i][0][1] =(prev[a][1][0], prev[a][1][1])

for i,j in sorted(rr1.items(),reverse=False,key=lambda x:x[0]):
    print(i[0]+","+i[1])