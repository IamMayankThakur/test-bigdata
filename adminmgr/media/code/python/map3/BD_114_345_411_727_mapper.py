#!/usr/bin/python3

import sys

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

venue = ''

for line in sys.stdin:

    line = line.strip()
    line = line.split(",")

    if(line[0] == "info" and line[1] == "venue"):
        venue = line[2]
        if(venue[0] == '"'):
            venue = venue + ',' +line[3]
    if(line[0] == "ball" and line[8] == "0"):
        b=int(line[7])
        c=1
        print('%s,%s\t%d,%d'%(venue,line[4],b,c))
