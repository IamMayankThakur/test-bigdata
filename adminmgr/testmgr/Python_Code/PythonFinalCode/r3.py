#!/usr/bin/python3
#batsman vulnerability
import csv
from operator import itemgetter
import sys
'''current_key = None
current_count = 0
current_ball_count=0
key = None'''
# input comes from STDIN
# input already sorted based on keys
L=[]
maxdict={}
for line in sys.stdin:
	#line = line.strip()
	line_val = line.split("\t")
	key, val = line_val[0], line_val[1]
	#print(val)
	values = val.split(",")
	if(int(values[2])==0):
		strike_rate=0.0
	else:
		strike_rate=(int(values[1])*100)/int(values[2])
		#print(strike_rate)
	if key not in maxdict:
		maxdict[key]=[values[0],[values[1],values[2]]]
		#print(maxdict[key])
	else:
		temp = maxdict[key]
		#print(temp)
		if(int(temp[1][1])==0):
			strikerateold=0.0
		else:
			strike_rateold= (int(temp [1][0]) * 100 ) / int(temp [1] [1])
		if strike_rate>strike_rateold:
			maxdict[key]=[values[0],[values[1],values[2]]]
		elif strike_rate==strike_rateold:
			if(values[1] > temp[1][0]):
				maxdict[key]=[values[0],[values[1],values[2]]]
				
#maxdict.sort()

for i in sorted(maxdict):
	temp = i + "," + maxdict[i][0]
	print(temp)
	
	'''
	#for i in val:
	keystr=str(key)
	valstr=str(val)
	r=keystr.split(',')
	rc=valstr.split(',')
	L.append([r[0],r[1],int(rc[0]),int(rc[1])])
	#batsman,bowler,wickets,deliveries
   	#SORT L BASED ON THE PROBLEM STATEMENT
L=sorted(L,key=lambda x: (-x[2],x[3],x[0]))
for i in range(len(L)):
	print(str(L[i][0])+","+str(L[i][1])+","+str(L[i][2])+","+str(L[i][3]))'''
#for line in sys.stdin:
#	line=line.strip()
#	key,val=line.split("\t",1)
#	print("%s,%s",key,val)
