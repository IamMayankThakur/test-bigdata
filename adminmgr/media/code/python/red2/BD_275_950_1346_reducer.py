#!/usr/bin/python3
"""reducer.py"""
from operator import itemgetter
import sys
import operator
# current_batsman = None
# current_count = 0
# current_run=0
# word = None
# d = dict()
# for line in sys.stdin:
# 	line = line.strip()
# 	batsman,runs,count = line.split('\t')
# 	try:
# 		count = int(count)
# 		runs=int(runs)
# 	except ValueError:
# 		continue

# 	if current_batsman == batsman:
# 		current_count += count
# 		current_run+=runs
# 	else:
# 		if current_batsman and current_count>5:
# 			d[current_batsman] = [current_run, current_count]
# 		current_run=runs
# 		current_count = count
# 		current_batsman = batsman

# if current_batsman == batsman and current_count>5:
# 	d[current_batsman] = [current_run, current_count]
# sorted_dict = sorted(d, key = lambda s: [-1*d[s][0], d[s][1]])
# for key in sorted_dict:
# 	batsman, bowler = key.split("*")
# 	print(bowler, batsman, d[key][0], d[key][1], sep=",")





d = {}
for line in sys.stdin:
	line = line.strip()
	batsman,runs,count = line.split('\t')
	try:
		count = int(count)
		runs=int(runs)
	except ValueError:
		continue
	if( batsman not in d.keys()):
		d[batsman] = [runs, count]
	else:
		d[batsman][0] += runs
		d[batsman][1] += count
ans = {}

for i in d.keys():
	if(d[i][1] > 5):
		ans[i] = d[i]

sorted_dict = sorted(ans, key = lambda x: [-ans[x][0], ans[x][1],x.split("*")[1],x.split("*")[1]])
for i in sorted_dict:
	batsman, bowler = i.split("*")
	print(bowler, batsman, ans[i][0], ans[i][1],sep=",")