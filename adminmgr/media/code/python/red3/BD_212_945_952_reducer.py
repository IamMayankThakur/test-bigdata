#!/usr/bin/python3

import sys
import csv

d1={}
for tup in sys.stdin:
	tup=eval(tup)
	keytup=(tup[0],tup[1])
	if keytup not in d1.keys() :
		d1.setdefault(keytup,[])
		d1[keytup].append(tup[2])
		d1[keytup].append(int(tup[3]))
	else:
		d1[keytup][0]+=tup[2]
		d1[keytup][1]+=int(tup[3])

for key in list(d1):
	if(d1[key][0]<10):
		d1.pop(key)
	else:
		runs=float(d1[key][1])
		delivery=d1[key][0]
		d1[key].append(runs*100/delivery)

l1=[]
for i in d1.keys():
	l1.append(i[0])

l1=list(set(l1))
l3=[]
for i in l1:
	l2=[]
	for j in d1.keys():
		if(j[0]==i):
			l2.append((j[1],d1[j][1],d1[j][2]))
	l2.sort(key= lambda k: k[1],reverse=True)
	l2.sort(key= lambda k1: k1[2],reverse=True)
	l3.append((i,l2[0][0]))

l3.sort(key= lambda k2: k2[0])

for i in l3:
	print('%s,%s' % (i[0],i[1]))







