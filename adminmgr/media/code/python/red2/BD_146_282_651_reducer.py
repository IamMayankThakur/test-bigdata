#!/usr/bin/env python3
import csv
from operator import itemgetter
import sys
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)
global current_key
global current_score
current_key = "" 
current_score = 0
dic = {}

for line in sys.stdin:
	line = line.strip()
	line_spl = line.split("\t")
	args = line_spl[0].split(",")
	key = (args[0],args[1]) 
	val1, val2 = line_spl[1], line_spl[2]
	try:
		score = int(val1)		
		deliveries = int(val2)
	except ValueError:
		continue

	if (current_key==key):
		dic[current_key][0] += score
		dic[current_key][1] += deliveries

	else:
		current_key = key
		current_score = 0
		if current_key not in dic:
			dic[current_key] = [0,0]
			dic[current_key][0] += score 
			dic[current_key][1] += deliveries 
		else:
			dic[current_key][0] += score
			dic[current_key][1] += deliveries


for key in list(dic):
	if(dic[key][1]<=5):
		del dic[key]
		
l = sorted(dic.items())
l1 = sorted(l, key = lambda x: x[1][1])
l2 = sorted(l1, key = lambda x: x[1][0], reverse = True)

for i in l2:
	print('%s,%s,%d,%d' % (i[0][0],i[0][1],int(i[1][0]),int(i[1][1])))





