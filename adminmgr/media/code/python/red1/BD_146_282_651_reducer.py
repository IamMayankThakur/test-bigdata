#!/usr/bin/env python3
import csv
from operator import itemgetter
import sys
from signal import signal, SIGPIPE, SIG_DFL
import copy
signal(SIGPIPE,SIG_DFL) 
global current_key
global current_count
#current_count1 = 0
#current_count2 = 0
current_key = ""
dic = {}
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	args = line_val[0].split(',')
	key = (args[0],args[1])
	val1, val2 = line_val[1] ,line_val[2]
	try:
		count1 = int(val1)
		count2 = int(val2)
	except ValueError:
		continue
	if current_key == key:
		dic[current_key][0] += count1
		dic[current_key][1] += count2
	else:
		current_key = key
		if current_key not in dic:
			dic[current_key] = [0,1]
			if(count1 == 1):
				dic[key][0] += count1
		else:
			dic[current_key][0] += count1
			dic[current_key][1] += count2
for key in list(dic):
	if(dic[key][1]<=5):
		del dic[key]

names_sort = sorted(dic.items())
balls_sort = sorted(names_sort, key = lambda kv: (kv[1][1]))
l2 = sorted(balls_sort, key = lambda kv: (kv[1][0]), reverse = True)


for key in l2:
	print('%s,%s,%s,%s' % (key[0][0],key[0][1],key[1][0],key[1][1]))



#print(dic)

