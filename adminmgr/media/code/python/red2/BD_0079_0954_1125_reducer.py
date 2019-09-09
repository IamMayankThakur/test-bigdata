#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

def ins_pos(output ,pair):
	if(len(output)>=1):
		i=0
		while((i<len(output)) and ((output[i])[1]>pair[1])):
			i+=1
		while((i<len(output)) and ((output[i])[2]<pair[2]) and ((output[i])[1]>=pair[1])):
			i+=1
		c=0
		while((i<len(output)) and (((output[i])[0])<pair[0]) and ((output[i])[2]<=pair[2]) and ((output[i])[1]>=pair[1])):
			i+=1
		output.insert(i,pair)
	else:
		output.append(pair)


inline = sys.stdin 

ball_count = 0
runs_count=0
current_pair = ""


output=list()
for line in inline:
    line = line.strip()
    line_val = line.split("\t")

    pair,runs,ball = line_val[0], line_val[1],line_val[2]
    try:
        runs= int(runs)
        ball=int(ball)
    except ValueError:
        continue

    if current_pair == pair:
    	ball_count+=ball
    	runs_count+= runs
    else:
        if(current_pair!=""):
            if((ball_count>5)):
                ins_pos(output,(current_pair,runs_count,ball_count))

        ball_count=1
        runs_count=runs
        current_pair =pair
if (current_pair!=""):
    if((ball_count>5)):
    	ins_pos(output,(current_pair,runs_count,ball_count))
    	# output.append((current_pair,bowled_count,ball_count))
for pair in output:
	print('%s,%d,%s' % (pair[0],pair[1],pair[2]))
