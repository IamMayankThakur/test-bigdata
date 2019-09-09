#!/usr/bin/python3


from itertools import groupby
from operator import itemgetter

import sys

mydict={} 
wickets = 0
runs = 0
balls = 0

for line in sys.stdin: 
	line = line.strip()
	batsman,bowler,run = line.split(',')
	key = (batsman,bowler)
	#[balls,wickets]
	if(key not in mydict):
		mydict[key]=[0,0]
	if(run=='-1'):
		mydict[key][1]+=1
	mydict[key][0]+=1
	

old = []
for i in sorted(mydict.items(),reverse = True,key = lambda x:x[1][1]) :
    old.append(i)
i = 0
j = 0
for k in range(len(old)-1):
  if(old[k][1][1] == old[k+1][1][1]):
    pass
  elif(old[k][1][1] != old[k+1][1][1]):
    old[i:k+1] = sorted(old[i:k+1],reverse = False,key =  lambda x:(x[1][0],x[0][0]))
    i = k+1
  
old[i:k+1] = sorted(old[i:k+1],reverse = False,key =  lambda x:(x[1][0],x[0][0]))
f=0
for i in old:
	if(i[1][0]>5):	
		print(",".join(i[0])+","+",".join([str(i[1][1]),str(i[1][0])]))

