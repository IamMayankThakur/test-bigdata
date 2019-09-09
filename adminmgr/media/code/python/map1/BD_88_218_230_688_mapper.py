#!/usr/bin/python3
import sys
pairs=[]
for line in sys.stdin:
	line = line.strip()
	line = line.split(",")
	if line[0]=="ball":
		batsman = line[4]
		bowler = line[6]
		wicket = line[9]
		pairs.append([batsman,bowler,wicket])

ctr=0
for i in pairs:
	if(i[2]=='caught' or i[2]=='bowled' or i[2]=='stumped' or i[2]=='lbw' or i[2]=='caught and bowled'or i[2]=='hit wicket' or i[2]=='obstructing the field'):
		ctr=1;
	else:
		ctr=0;
	i[2]=ctr;
a=sorted(pairs,key=lambda x:x[0].lower());		
for i in a:
	print(i[0],i[1],i[2],sep=",");

#returns (batsman,bowler,wicket)
#wicket is returned as 0 if its run out or retired run
#wicket is returned as 1 if its anything else
