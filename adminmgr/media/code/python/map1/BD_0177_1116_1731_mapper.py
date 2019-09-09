#!/usr/bin/python3
import sys
for line in sys.stdin:
	line=line.split(",")
	if(len(line)>9): 
		if(line[9]!='run out'and line[9]!='""' and line[9]!='retired hurt'):
			temp=line[4]+","+line[6]
			print(temp,"_",1,"_",1)
		else:
			temp=line[4]+","+line[6]
			print(temp,"_",0,"_",1)
			

