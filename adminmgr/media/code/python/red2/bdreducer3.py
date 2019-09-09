#!/usr/bin/python3
from operator import itemgetter
import sys
import ast

final = {}

for line in sys.stdin:
    res = ast.literal_eval(line)
    tup = (res[0] ,res[1])
    if tup in final:
	final[tup][0] += res[2]
	final[tup][1] += res[3]
    else:
	final[tup] = [res[2], res[3]]
lis=[]
for item in final.items():
	lis.append([item[0][0], item[0][1], -1 * item[1][0],item [1][1]])
lis = sorted(lis, key=itemgetter(2, 3, 0))
for i in lis:
	i[2] *= (-1)
for i in lis:
        if(i[3] > 5):
		print('%s,%s,%s,%s'%(i[0],i[1],i[2],i[3]))
