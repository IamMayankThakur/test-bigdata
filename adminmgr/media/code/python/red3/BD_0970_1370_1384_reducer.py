#!/usr/bin/python3
#Reducer.py
import sys
import operator

strike = {}
result = {}
res = {}

#Partitoner
for line in sys.stdin:
	line = line.strip()
	venbat, rundel = line.split('\t')
	venue,bat = venbat.split('/')
	run,ball = rundel.split('/')
	run = int(run,10)
	ball = int(ball,10)
	if venue not in strike:
		strike[venue] = {}
		result[venue] = {}
	if(bat not in strike[venue]):
			strike[venue][bat]=[run,ball]
	else:
			strike[venue][bat][0]+=run
			strike[venue][bat][1]+=ball
	

#Reducer
for ven in strike:
	for bat in strike[ven]:
		if strike[ven][bat][1]>=10:
			result[ven][bat]=strike[ven][bat][0]*100/strike[ven][bat][1]
for ven in result:
	l = [k for k,v in result[ven].items() if v == max(result[ven].values())]

	if len(l) ==1:
		res[ven] = l[0]
	else:
		r = 0
		for b in l:
			if strike[ven][b][0] >r:
				r = strike[ven][b][0]
				ba = b
				res[ven]=ba
res = dict(sorted(res.items(), key=operator.itemgetter(0)))
for ven in res:
	print ('%s,%s'% (ven,res[ven]))


