#!/usr/bin/python3
import sys
fileread = sys.stdin
for line in fileread:
	mylist=line.split(",")
	j=len(mylist)
	if(j<9):
		pass
	else:
		if(mylist[9]!='run out' and mylist[9]!='""' and mylist[9]!='retired hurt'):
			print(mylist[4],mylist[6],1,1,sep="$")
		else:
			print(mylist[4],mylist[6],0,1,sep="$")

