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
		d1[keytup].append(tup[3])
	else:
		d1[keytup][0]+=tup[2]
		d1[keytup][1]+=tup[3]


for key in list(d1):
	if(d1[key][0]<6):
		d1.pop(key)


l1=sorted(d1.items(),key= lambda k1 : k1[0][1])
l1.sort(key= lambda k4 : k4[0][0])
l1.sort(key= lambda k2 : k2[1][0])
l1.sort(key= lambda k3 : k3[1][1],reverse=True)

for i in l1:
	print('%s,%s,%d,%d' % (i[0][0],i[0][1],i[1][1],i[1][0]))
	
			

