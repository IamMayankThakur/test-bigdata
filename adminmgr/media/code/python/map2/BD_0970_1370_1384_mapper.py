#!/usr/bin/python3

import sys
result = {}

for line in sys.stdin:
	line = line.strip()
	line = line.split(",")
	if(line[0] == "ball"):
		bat = line[4]
		bowl = line[6]
		b = bowl+"/"+bat
		if b not in result:
			result[b]=[int(line[7],10)+int(line[8],10),1]
		else:
			result[b][0]=result[b][0]+int(line[7],10)+int(line[8],10)
			result[b][1]+=1

for bat in result:
	print("%s\t%s"%(bat,str(result[bat][0])+"/"+str(result[bat][1])))
	

