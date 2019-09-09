#!/usr/bin/python3
import sys
stkrate = {}

for row in sys.stdin:

	row = row.strip()
	row = row.split(",")

	if(row[1] == "venue"):

		venue = row[2]
		if(len(row) ==4):		
			venue = venue+","+row[3]
		
	if(row[0] == "ball" and int(row[8])==0):
		bat = row[4]
		venbat = venue+"&"+bat
		if(venbat not in stkrate):
			y=int(row[7])
			stkrate[venbat]=[y,1]

		else:

			x=int(row[7])
			stkrate[venbat][0]+=x
			stkrate[venbat][1]+=1

			
for place in stkrate:

	print("%s$%s"%(place,str(stkrate[place][0])+";"+str(stkrate[place][1])))
	


