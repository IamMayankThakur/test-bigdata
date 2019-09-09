#!/usr/bin/python3
import sys
#n=int(input())
dic={}
for line in sys.stdin:
	row=list(map(str,line.split(",")))
	extras=int(row[3][:-1])
	if (row[0],row[1]) not in dic:
		dic[(row[0],row[1])]=[int(row[2]),1]
		dic[(row[0],row[1])][0]+=int(extras)
	else:
		dic[(row[0],row[1])][0]+=int(row[2])+int(extras)
		dic[(row[0],row[1])][1]+=1

for i in sorted(dic,key=lambda k:(-dic[k][0],dic[k][1],k[0])):
	print(i[0],i[1],dic[i][0],dic[i][1],sep=",")