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
	stadium,bat,run,extras = line.split('|')
	key =stadium

	if(key not in mydict):
		mydict[key]={}
	if(bat not in mydict[key]):
		mydict[key][bat]=[0,0,0] #run,balls,rate
	if(extras=='0'):
		#print("dsf")
		mydict[key][bat][0]+=int(run)
		mydict[key][bat][1]+=1
		mydict[key][bat][2]=mydict[key][bat][0]/mydict[key][bat][1]		

	#for i in mydict:
		#print(i,mydict[i])
result={}
for i in mydict: #stadium=i
	#print(mydict[i])
	if(i not in result):
		result[i]=["",0,0] #batsman,run,rate
	for j in mydict[i]: #j=batsman in particular(i) stadium played
		if(mydict[i][j][1]>=10 and result[i][2]<mydict[i][j][2]):
			result[i][0]=j
			result[i][1]=mydict[i][j][0]
			result[i][2]=mydict[i][j][2]
		elif(mydict[i][j][1]>=10 and result[i][2]==mydict[i][j][2] and result[i][1]<mydict[i][j][1]):					
			result[i][0]=j
			result[i][1]=mydict[i][j][0]
			result[i][2]=mydict[i][j][2]
#print(result)
for i in sorted(result.keys()):
	print(i+","+result[i][0])

