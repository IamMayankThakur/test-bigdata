#!/usr/bin/python3

import sys

batbowl = ()
mydict = {}

for line in sys.stdin:
	line = line.strip()
	batsman, bowler, runs = line.split(',')
	runs = int(runs)
	batbowl = (batsman,bowler)
		
	if(batbowl in mydict):
		mydict[batbowl] = (mydict[batbowl][0]+runs,mydict[batbowl][1]+1)
	else:
		mydict[batbowl] = (runs,1)

mydict = sorted(mydict.items(),key=lambda item: (-item[1][0], item[1][1], item[0][1]))	

for key, value in mydict:
	if(value[1] > 5):
	    print("%s,%s,%d,%d" % (key[1],key[0], value[0],value[1]))
