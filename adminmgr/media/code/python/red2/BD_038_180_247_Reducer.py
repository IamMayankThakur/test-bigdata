#!/usr/bin/python3
import csv
from operator import itemgetter
import sys


c=0
d={}
for line in sys.stdin:
	line=line.strip()
	
	lines=line.split("\t")
	
	x=(lines[0],lines[1])

	if x in d:
		y=int(lines[2]) + int(lines[3])
		d[x][0]+=y
		d[x][1]+=1
	else:
		#print(c)
		#print(type(lines[2]),type(lines[3]))
		y=int(lines[2]) + int(lines[3])
		#print(type(y))
	
		d[x]=[y,int(lines[4])]
	
	#print(c)

l=[]
for i in d.items():
	k=i[0]
	v=i[1]
	if(v[1]>5):
		l.append((k[1],k[0],v[0],v[1]))

l1=sorted(l,key=lambda x:(-x[2],x[3],x[0]))
for i in l1:
	
	print('%s,%s,%d,%d' % (i[0],i[1],i[2],i[3]))
