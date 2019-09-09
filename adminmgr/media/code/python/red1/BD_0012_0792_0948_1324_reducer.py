#!/usr/bin/python3
import sys
from operator import itemgetter
import ast

first = {}
for ln in sys.stdin:
    sectuple = ast.literal_eval(ln)
    t = (sectuple[0], sectuple[1])
    if t not in first:
        first[t] = [sectuple[2], sectuple[3]]

    else:
        first[t][0] += sectuple[2]
        first[t][1] += sectuple[3]	
		
last = []
for item in first.items():
    last.append([item[0][0], item[0][1], -1 * item[1][0], item[1][1]])
last = sorted(last, key=itemgetter(2, 3, 0))
for x in last:
    x[2] *= (-1)
for x in last:
    if(x[3] > 5):
        print('%s,%s,%s,%s'%(x[0],x[1],x[2],x[3]))
