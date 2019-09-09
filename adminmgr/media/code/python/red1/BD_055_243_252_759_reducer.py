#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
def task2(op ,p):
	if(len(op)>=1):
		i=0
		while((i<len(op)) and ((op[i])[1]>p[1])):
			i+=1
		while((i<len(op)) and ((op[i])[2]<p[2]) and ((op[i])[1]>=p[1])):
			i+=1
		c=0
		while((i<len(op)) and (((op[i])[0])<p[0]) and ((op[i])[2]<=p[2]) and ((op[i])[1]>=p[1])):
			i+=1
		op.insert(i,p)
	else:
		op.append(p)
inline = sys.stdin 
ballcount = 0
wicket=0
cp = ""
op=list()
for line in inline:
    line = line.strip()
    line_val = line.split("\t")
    p,bowled,ball = line_val[0], line_val[1],line_val[2]
    try:
        bowled = int(bowled)
        ball=int(ball)
    except ValueError:
        continue
    if cp == p:
    	ballcount+=ball
    	wicket+= bowled
    else:
        if(cp!=""):
            if((ballcount>5)):
                task2(op,(cp,wicket,ballcount))
        ballcount=1
        wicket=bowled
        cp =p
if (cp!=""):
    if((ballcount>5)):
    	task2(op,(cp,wicket,ballcount))
for p in op:
	print('%s,%d,%s' % (p[0],p[1],p[2]))
