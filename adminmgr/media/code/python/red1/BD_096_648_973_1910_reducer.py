#!/usr/bin/env python3
"""reducer.py"""

#from operator import itemgetter
import sys
x={}
for line in sys.stdin:
	#line=line.strip()
	l=line.split('\t')
	k=l[0]+"#"+l[1]
	if(k in x.keys()):
		x[k][0]+=int(l[2])
		x[k][1]+=1
	else:
		x[k]=[0,1]
		x[k][0]+=int(l[2])
#print(x)
y=sorted(x.items(),key=lambda x:(-x[1][0],x[1][1],x[0]))
for a in y:
	if(a[1][1]>5):
		print(a[0].split('#')[0],a[0].split('#')[1],a[1][0],a[1][1],sep=",")
