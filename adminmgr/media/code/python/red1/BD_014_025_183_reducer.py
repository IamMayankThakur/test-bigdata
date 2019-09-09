#!/usr/bin/python3

import sys

countWick = 0
countBalls = 0
batbowl = ()
wickball = ()
mydict = {}

for line in sys.stdin:
	line = line.strip()
	batsman, bowler, wick, ball = line.split(',')
	
	wick = int(wick)
	ball = int(ball)
	
	batbowl = (batsman,bowler)
	wickball = (wick,ball)
	
	if(batbowl in mydict):
		mydict[batbowl] = (mydict[batbowl][0]+wick, mydict[batbowl][1]+ball)
	else:
		mydict[batbowl] = wickball

mydict = sorted(mydict.items(), key=lambda item: (-item[1][0], item[1][1],item[0][0]))		

for key, value in mydict:
	if(value[1] > 5):
	    print("%s,%s,%d,%d" % (key[0],key[1], value[0],value[1]))
