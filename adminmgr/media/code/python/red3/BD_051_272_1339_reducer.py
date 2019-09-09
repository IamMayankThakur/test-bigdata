#!/usr/bin/python3
import sys
dic_init={}
for line in sys.stdin:
	row=list(map(str,line.split("|")))
	score=int(row[2][:-1])
	if (row[0],row[1]) not in dic_init:
		dic_init[(row[0],row[1])]=[score,1]
	else:
		dic_init[(row[0],row[1])][0]+=score
		dic_init[(row[0],row[1])][1]+=1
dic_strike_rate={}
for i in dic_init:
	strike_rate=(dic_init[i][0]*100)/dic_init[i][1]
	if i[0] not in dic_strike_rate:
		dic_strike_rate[i[0]]=[]
	else:
		dic_strike_rate[i[0]].append((i[1],strike_rate,dic_init[i][0]))
	#print(row)}
#print(dic_strike_rate)
for i in sorted(dic_strike_rate,key=lambda k:k):
	j=sorted(dic_strike_rate[i],key=lambda k:(-k[1],-k[2]))[0]
	print(i,j[0],sep=",")

