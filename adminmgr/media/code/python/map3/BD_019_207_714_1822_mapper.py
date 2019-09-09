#!/usr/bin/python3
import sys
fileread=sys.stdin
for line in fileread:
	array_third=line.strip()
	mylist=array_third.split(',')
	if(len(mylist)<7):
		if(mylist[1]=='venue'):
			if(len(mylist)==3 and mylist[1]=='venue'):
				stadium=mylist[2]
			if(len(mylist)==4 and mylist[1]=='venue'):
				stadium=str(str(mylist[2])+","+str(mylist[3]))
	else:
		if(mylist[8]!='0'):
			pass
		else:
			j=int(mylist[7])	
			print(stadium,mylist[4],j,1,sep='*')
