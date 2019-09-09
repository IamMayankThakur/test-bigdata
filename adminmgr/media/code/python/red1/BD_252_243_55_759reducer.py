#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
def ins_pos(op ,p):
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
bc = 0
count=0
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
    	bc+=ball
    	count+= bowled
    else:
        if(cp!=""):
            if((bc>5)):
                ins_pos(op,(cp,count,bc))
        bc=1
        count=bowled
        cp =p
if (cp!=""):
    if((bc>5)):
    	ins_pos(op,(cp,count,bc))
for p in op:
	print('%s,%d,%s' % (p[0],p[1],p[2]))
