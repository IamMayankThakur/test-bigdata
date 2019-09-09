#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
def ins_pos(op ,pair):
	if(len(op)>=1):
		i=0
		while((i<len(op)) and ((op[i])[1]>pair[1])):
			i+=1
		while((i<len(op)) and ((op[i])[2]<pair[2]) and ((op[i])[1]>=pair[1])):
			i+=1
		c=0
		while((i<len(op)) and (((op[i])[0])<pair[0]) and ((op[i])[2]<=pair[2]) and ((op[i])[1]>=pair[1])):
			i+=1
		op.insert(i,pair)
	else:
		op.append(pair)
inline = sys.stdin 
bc = 0
rc=0
cp = ""
op=list()
for line in inline:
    line = line.strip()
    line_val = line.split("\t")
    pair,runs,ball = line_val[0], line_val[1],line_val[2]
    try:
        runs= int(runs)
        ball=int(ball)
    except ValueError:
        continue
    if cp == pair:
    	bc+=ball
    	rc+= runs
    else:
        if(cp!=""):
            if((bc>5)):
                ins_pos(op,(cp,rc,bc))
        bc=1
        rc=runs
        cp =pair
if (cp!=""):
    if((bc>5)):
    	ins_pos(op,(cp,rc,bc))
for pair in op:
	print('%s,%d,%s' % (pair[0],pair[1],pair[2]))
