#!/usr/bin/python3
import sys
#n=int(input())
pair={}
for ln in sys.stdin:
	col=list(map(str,line.split(",")))
	extras=int(row[3][:-1])
	if (col[0],col[1]) not in dic:
		pair[(col[0],col[1])]=[int(col[2]),1]
		pair[(col[0],col[1])][0]+=int(extras)
	else:
		pair[(col[0],col[1])][0]+=int(col[2])+int(extras)
		pair[(col[0],col[1])][1]+=1

for i in sorted(dic,key=lambda k:(-pair[k][0],pair[k][1],k[0])):
	print(i[0],i[1],pair[i][0],pair[i][1],sep=",")