#!/usr/bin/python3
import sys
pair={}
for ln in sys.stdin:
	col=ln.strip()
	col=ln.split(",")
	ex=int(col[3][:-1])
	if(col[0],col[1]) in pair:
		pair[(col[0],col[1])][0]=pair[(col[0],col[1])][0]+int(col[2])+int(ex)
		pair[(col[0],col[1])][1]+=1
	else:
		pair[(col[0],col[1])]=[int(col[2]),1]
for data in sorted(pair,key=lambda i:(-pair[i][0],pair[i][1],i[0])):
	print(data[0],data[1],pair[data][0],pair[data][1],sep=",")