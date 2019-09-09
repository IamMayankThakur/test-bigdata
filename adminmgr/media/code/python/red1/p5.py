#!/usr/bin/python3
import sys
import operator


set = {}
result = {}


for a in sys.stdin:
	
	bbw,ball = a.split(';')
	batsmanbolwer,wicket = bbw.split('$')
	
	if batsmanbolwer not in set:

		set[batsmanbolwer] = [int(wicket),int(ball)]
		
	else:

			set[batsmanbolwer][0]=set[batsmanbolwer][0]+wicket
			set[batsmanbolwer][1]=set[batsmanbolwer][1]+ball


a = [(key,value) for key,value in set.items() if (value[1]>5)]
result=dict(a)

b=sorted(result.items(), key = lambda i: i[1] ,reverse = True)
result=dict(b)


m = -1

for bat in result:

	if(result[bat][0]!=m):

		q = [(key,value[1]) for key,value in result.items() if (value[0] == result[bat][0])]
		q = sorted(q,key = lambda i: i[0])
		r = sorted(q,key = lambda i: i[1])
		m = result[bat][0]

		for j in r:

			bo,b = j[0].split('&')
			print("%s,%s,%d,%d"%(bo,b,result[j[0]][0],result[j[0]][1]))
	
	

