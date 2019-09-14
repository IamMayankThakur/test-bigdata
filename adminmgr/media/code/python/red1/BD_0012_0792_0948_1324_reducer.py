#!/usr/bin/python3
from operator import itemgetter
import sys
import ast

temp = dict()

for string in sys.stdin:
	result = ast.literal_eval(string)
	t = (result[0], result[1])
	if t not in temp:
		temp[t]=[result[2],result[3]]
	else:
		temp[t][0] += result[2]
		temp[t][1] += result[3]
l = []
for i in temp.items():
	l.append([i[0][0], i[0][1], -1 * i[1][0], i[1][1]])
	l = sorted(l, key=itemgetter(2, 3, 0))
for i in l:
	i[2] *= (-1)
for i in l:
	if(i[3] > 5):
		print('%s,%s,%s,%s'%(i[0],i[1],i[2],i[3]))
