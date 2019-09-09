#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

results = dict()
for line in sys.stdin:
	line = line.strip()
	info = line.split(":")
	#print(info)
	deliv = info[1]
	pair = info[0].split(",")
	#print(pair)
	key_pair= (pair[0],pair[1])
	wicket = int(pair[2])

	if key_pair in results:
		results[key_pair]["no_of_delivs"]+= 1
		if(wicket):
			results[key_pair]["no_of_wickets"]+=1
	else:
		results[key_pair]={"no_of_wickets":0, "no_of_delivs":1}
		if(wicket):
			results[key_pair]["no_of_wickets"] += 1

for key in list(results):
	if results[key]["no_of_delivs"] <= 5:
		del results[key]


sorted_result = sorted(sorted(sorted(results.items()), key = lambda x: x[1]["no_of_delivs"]), key = lambda x: x[1]["no_of_wickets"], reverse = True)
for pairs in sorted_result:
	print ("%s,%s,%d,%d" % (str(pairs[0][0]),str(pairs[0][1]),(pairs[1]["no_of_wickets"]),(pairs[1]["no_of_delivs"])))

