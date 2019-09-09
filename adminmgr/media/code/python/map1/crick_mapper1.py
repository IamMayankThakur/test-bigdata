#!/usr/bin/env python

"""

0.    Ball
1.    Innings
2.    Over.Ball of the over
3.    Batting Team
4.    Batsman on strike
5.    Batsman on non-striker's end
6.    Bowler
7.    Runs scored on that ball
8.    Extra
9.    Nature of dismissal
10.   Batsman dismissal

"""




import sys

for record in sys.stdin:

    record = record.strip()

    values = record.split(",")

    if(values[0] == 'ball'):
        #print(str(values) + '\t' + str(len(values)));

        if(values[4] == values[10] and values[9]!="run out" and values[9]!="retire hurt"):
		a=1
		b=1		
		print('%s,%s:%d,%d'%(values[4],values[6],a,b))
        else:
		a=0
		b=1
		print('%s,%s:%d,%d'%(values[4],values[6],a,b))
