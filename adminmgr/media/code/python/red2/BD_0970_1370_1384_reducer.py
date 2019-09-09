#!/usr/bin/python3
#Reducer.py
import sys
import operator


result = {}
res = {}

#Partitoner
for line in sys.stdin:
	line = line.strip()
	batbowl, rundel = line.split('\t')
	run,ball = rundel.split('/')
	run = int(run,10)
	ball = int(ball,10)
	if batbowl not in result:
		result[batbowl] = [run,ball]
		
	else:
			result[batbowl][0]+=run
			result[batbowl][1]+=ball
	
#print(result)
#Reducer
res = dict([(key,val) for key,val in result.items() if val[1]>5])
res = dict(sorted(res.items(), key=operator.itemgetter(1),reverse = True))

va = -1
for bat in res:
	if(res[bat][0]!=va):
	
		l = [(k,v[1]) for k,v in res.items() if v[0] == res[bat][0]]
		l = sorted(l,key = lambda i: i[0])
		r = (sorted(l,key = lambda i: i[1]))
		va = res[bat][0]
		for v in r:
			bowl,bats = v[0].split('/')
			print("%s,%s,%d,%d"%(bowl,bats,res[v[0]][0],res[v[0]][1]))
	
	

