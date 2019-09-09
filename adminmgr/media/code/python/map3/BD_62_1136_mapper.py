#!/usr/bin/python3 
import sys
import csv
content = sys.stdin
count=0
mi=[]

for line in content:
	line = line.strip()
	mylist = line.split(",")
	if (mylist[0]=='info' and mylist[1]=='venue'):
		if mylist[3]=='':
			v=mylist[2]
		else:
			v=mylist[2:4]
			sep=','
			v=sep.join(v)

	if(mylist[0]=='ball'):
		bt=mylist[4]
		runs=mylist[7]
		runs=int(runs)
		ex=mylist[8]
		ex=int(ex)
		if ex==0:
			print(v,bt,runs,1,sep='$')

