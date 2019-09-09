#!/usr/bin/python3
from operator import itemgetter
import sys
import ast
lis_=[]
final = {}

for line in sys.stdin:
    res = ast.literal_eval(line)
    tup = (res[0], res[1])
    if tup in final:
        final[tup][0] += res[2]
        final[tup][1] += res[3]
    else:
        final[tup] = [res[2], res[3]]

for x in final.items():
    lis_.append([x[0][0], x[0][1], -1 * x[1][0], x[1][1]])
lis_ = sorted(lis_, key=itemgetter(2, 3, 0))
for j in lis_:
    j[2] *= (-1)
for i in lis_:
    if(i[3] > 5):
        print('%s,%s,%s,%s'%(i[0],i[1],i[2],i[3]))
