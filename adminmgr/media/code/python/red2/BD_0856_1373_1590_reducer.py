#!/usr/bin/python3
import sys
import operator


set = {}
result = {}


for a in sys.stdin:

	bbr,ball = a.split(';')
	bowlerbatsman,runs = bbr.split('$')

	if bowlerbatsman not in set:
		set[bowlerbatsman] = [int(runs),int(ball)]

		
	else:
			set[bowlerbatsman][0]+=runs
			set[bowlerbatsman][1]+=ball


y = [(key,value) for key,value in set.items() if value[1]>5]
result=dict(y)

z=sorted(result.items(), key=lambda kv: kv[1],reverse = True)
result = dict(z)

m=0
for n in result:

	if(result[n][0]!=m):


		q = [(k,v[1]) for k,v in result.items() if v[0] == result[n][0]]

		q = sorted(q,key = lambda i: i[0])
		r = sorted(q,key = lambda i: i[1])
		m = result[n][0]

		for j in r:

			bowler,bm = j[0].split('&')
			print("%s,%s,%d,%d"%(bowler,bm,result[j[0]][0],result[j[0]][1]))
