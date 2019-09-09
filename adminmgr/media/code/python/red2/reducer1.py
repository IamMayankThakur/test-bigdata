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
		d[x][0]+=1
		#print(lines[3])
		if(int(lines[3])==1):
			#print("ok")
			d[x][1]+=1
	else:
		#print(lines[2],lines[3])
		d[x]=[int(lines[2]),int(lines[3])]
	
#print(d)
l=[]
for i in d.items():
	#print(i)
	k=i[0]
	v=i[1]
	#print(k,v[0])
	if(v[0]>5):
		#print(k[0],k[1],v[1],v[0])
		l.append((k[0],k[1],v[1],v[0]))


l1=sorted(l,key=lambda x:(x[2],-x[3],x[0]))
for i in l1:
	print('%s,%s,%d,%d' % (i[0],i[1],i[2],i[3]))



#print("ppp")

#print(l1[0],l1[10])
 
