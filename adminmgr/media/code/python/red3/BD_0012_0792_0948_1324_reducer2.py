#!/usr/bin/python3
from operator import itemgetter
import sys
import ast

first = {}

def get_zero_index(listt):
    return [i[0] for i in listt]

for ln in sys.stdin:
    sectuple = ast.literal_eval(ln)
    t = (sectuple[0], sectuple[1])
    if t in first:
        first[t][0] += sectuple[2]
        first[t][1] += sectuple[3]
    else:
        first[t] = [sectuple[2], sectuple[3]]

listt = []

for item in first.items():
    listt.append([item[0][0], item[0][1], (-1) * item[1][0], item[1]
                [1], (-1) * (item[1][0]/float(item[1][1]) * 100)])
    listt = sorted(listt, key=itemgetter(0, 4, 2, 1))

for x in listt:
    x[2] *= (-1)
    x[4] *= (-1)

last_list = []

for y in listt:
    if((y[0] not in get_zero_index(last_list)) and y[3] >= 10):
        last_list.append(y)

for z in last_list:
    print('%s,%s' % (z[0], z[1]))
        
