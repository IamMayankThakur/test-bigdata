#!/usr/bin/python3
import sys

diction={}

def func1(x):
	return diction[x][0]

def func2(x):
	return diction[x][1]

def func3(x):
	return x[0]

for line in sys.stdin:
	line=line.strip()
	line_val=line.split(',')
	bowl,bat,runs,balls=line_val
	runs=int(runs)
	print(runs)
	balls=int(balls)
	key=(bowl,bat)
if key in diction:
	diction[key][0].append(runs)
	diction[key][1].append(balls)
else:
	diction[key]=[[],[]]
	diction[key][0].append(runs)
	diction[key][1].append(balls)
	

for key in diction.keys();
	diction[key][0]=sum(diction[key][0])
	diction[key][1]=sum(diction[key][1])

s=sorted(diction,key=func3)
s=sorted(s,key=func2)
s=sorted(s,key=func1,reverse=True)

for k in s:
if diction[k][1]>5:
	print('%s,%s,%d,%d' % (k[0],k[1],diction[k][0],diction[k][1]))   
