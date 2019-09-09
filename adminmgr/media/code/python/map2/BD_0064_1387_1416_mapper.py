#!/usr/bin/python3
import csv
import sys

fields = []
rows = []
for row in sys.stdin:
	row=row.strip()
	row1=list(row.split(","))
	rows.append(row1)


task1={}
result={}
#177391

for row in rows:
	if row[0]=="ball":
		a=row[4]
		b=row[6]
		if (b,a) not in task1:
			task1[(b,a)]=[0]*2
			task1[(b,a)][1]+=1
			task1[(b,a)][0]+=(int(row[7])+int(row[8]))
			
		else:
			task1[(b,a)][1]+=1

			task1[(b,a)][0]+=(int(row[7])+int(row[8]))
#without sorting:


for v in task1:
	print(v[0],end=",")
	print(v[1],end=":")
	print(task1[v][0],end=",")
	print(task1[v][1])



