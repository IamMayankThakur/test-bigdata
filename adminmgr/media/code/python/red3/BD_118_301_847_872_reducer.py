#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

results=dict()
li=[]
for line in sys.stdin:
	line = line.strip()
	info = line.split(":")
	stadium = info[0]
	bats_runs = info[1].split(",")
	batsman = bats_runs[0]
	runs = int(bats_runs[1])
	if stadium in results:
		if batsman not in results[stadium]:
			results[stadium][batsman]={"no_of_runs":0, "no_of_delivs":1,"strike":0}
			results[stadium][batsman]["no_of_runs"]+=runs
		else:
			results[stadium][batsman]["no_of_delivs"]+=1
			results[stadium][batsman]["no_of_runs"]+=runs
	else:
		results[stadium]={}
		results[stadium][batsman]={"no_of_runs":0, "no_of_delivs":1, "strike":0}
		results[stadium][batsman]["no_of_runs"]+=runs



for stadium in list(results):
	for batter in list(results[stadium]):
		if results[stadium][batter]["no_of_delivs"] < 10:
			del results[stadium][batter]


for stadium in results:
	for batter in results[stadium]:
		strike = (results[stadium][batter]["no_of_runs"]*100)/results[stadium][batter]["no_of_delivs"]
		results[stadium][batter]["strike"] = strike
		

fin={}
for stadium in results:
	max_sr=0
	for batter in results[stadium]:
		if(results[stadium][batter]["strike"]>=max_sr):
			if(results[stadium][batter]["strike"]==max_sr):
				recent=fin[stadium]
				if(results[stadium][recent]["no_of_runs"]<results[stadium][batter]["no_of_runs"]):

					max_sr=results[stadium][batter]["strike"]
					fin[stadium]=batter
	
			else:	
				max_sr=results[stadium][batter]["strike"]
				fin[stadium]=batter
res=sorted(fin.items())
for i in res:
	print(i[0]+","+str(i[1]))




