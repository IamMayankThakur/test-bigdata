#!/usr/bin/python3

import sys
data=""
for line in sys.stdin:
	line=line.strip()
	line=line.split(',')
	if len(line)<7:
		if line[1]=="venue":
			if len(line)==3 and line[1]=="venue":
				data=line[2]
			if len(line)==4 and line[1]=="venue":
				data=str(str(line[2])+","+str(line[3]))
	else:
		if line[8]=='0':
			j=int(line[7])	
			print(data,line[4],j,1,sep='_')
