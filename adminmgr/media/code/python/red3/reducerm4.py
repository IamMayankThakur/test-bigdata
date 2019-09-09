#!/usr/bin/python3
from operator import itemgetter
import sys
import ast

final = {}

def get_zero_index(lis):
    return [i[0] for i in lis]

for line in sys.stdin:
    res = ast.literal_eval(line)
    tup = (res[0], res[1])
    if tup in final:
        final[tup][0] += res[2]
        final[tup][1] += res[3]
    else:
        final[tup] = [res[2], res[3]]

lis = []

for item in final.items():
    lis.append([item[0][0], item[0][1], (-1) * item[1][0], item[1]
                [1], (-1) * (item[1][0]/float(item[1][1]) * 100)])
    lis = sorted(lis, key=itemgetter(0, 4, 2, 1))

for i in lis:
    i[2] *= (-1)
    i[4] *= (-1)

top_list = []

for i in lis:
    if((i[0] not in ([val[0] for val in top_list])) and i[3] >= 10):
        top_list.append(i)

for i in top_list:
    print('%s,%s' % (i[0], i[1]))
        
