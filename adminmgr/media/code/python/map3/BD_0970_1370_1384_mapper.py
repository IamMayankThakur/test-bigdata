#!/usr/bin/python3

import sys
strike = {}
# input comes from STDIN (standard input)
for line in sys.stdin:
	line = line.strip()
	line = line.split(",")
	if(line[1] == "venue"):
		venue = line[2]
		if(len(line) ==4):		
			venue = venue+","+line[3]
		
	if(line[0] == "ball" and int(line[8],10) ==0):
		bat = line[4]
		venbat = venue+"/"+bat
		if(venbat not in strike):
			strike[venbat]=[int(line[7],10),1]
		else:
			strike[venbat][0]+=int(line[7],10)
			strike[venbat][1]+=1

			
for ven in strike:
	print("%s\t%s"%(ven,str(strike[ven][0])+"/"+str(strike[ven][1])))
	


