#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

results = dict()
for line in sys.stdin:
	line = line.strip()
	info = line.split(":")
	deliv = info[1]
	pair = info[0].split(",")
	key_pair= (pair[0],pair[1]) #bowler, batsman
	runs = int(pair[2])
	if key_pair in results:
		results[key_pair]["no_of_delivs"]+= 1
		results[key_pair]["no_of_runs"]+=runs
	else:
		results[key_pair]={"no_of_runs":0, "no_of_delivs":1}
		results[key_pair]["no_of_runs"] += runs

for key in list(results):
	if results[key]["no_of_delivs"] <= 5:
		del results[key]
    
sorted_result = sorted(sorted(sorted(results.items()), key = lambda x: x[1]["no_of_delivs"]), key = lambda x: x[1]["no_of_runs"], reverse = True)
for pairs in sorted_result:
	print(str(pairs[0][0])+","+str(pairs[0][1])+"," + str(pairs[1]["no_of_runs"])+","+str(pairs[1]["no_of_delivs"]))
