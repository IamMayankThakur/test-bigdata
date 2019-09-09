#!/usr/bin/python3
import sys
for line in sys.stdin:
	line=line.split(",")
	if(len(line)>9): 
		line[6]=line[6]+","
		print(line[6]+line[4],"_",int(line[7])+int(line[8]),"_",1)
