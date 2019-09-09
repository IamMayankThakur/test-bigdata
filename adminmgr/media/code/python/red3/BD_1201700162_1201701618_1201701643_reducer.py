#!/usr/bin/python3
#Reducer.py
import sys
import operator

rec={}
for record in sys.stdin:
	vb,sr=record.split('\t')
	sdn,bats=vb.split('/')
	sr=sr.split('/')
	balls=int(sr[1],10)
	runs=int(sr[0],10)
	if sdn not in rec:
		rec[sdn]={}
	if bats not in rec[sdn]:
		rec[sdn][bats]=[]
		rec[sdn][bats].append(runs)
		rec[sdn][bats].append(balls)
	else:
		rec[sdn][bats][0]+=runs
		rec[sdn][balls][1]+=balls
		
new={}
for key,value in rec.items():
	new[key]={}
	for bats,sr in value.items():
		if sr[1]>=10:
			new[key][bats]=[]
			sr_new=sr[0]*100/sr[1]
			new[key][bats].append(sr_new)
			new[key][bats].append(sr[0])

final={}
for stname,players in new.items():
	batsmen= None
	l = [k for k,v in players.items() if v == max(players.values())]
	if len(l)>1:
		maxdel=0
		if new[stname][players][1]>maxdel:
			maxdel=new[stname][players][1]
			final[stname]=l[0]
	else:		
		final[stname]=l[0]
res = dict(sorted(final.items(), key=operator.itemgetter(0)))
for fin,batsmen in res.items():
	print ('%s,%s'% (fin,batsmen))
			
				
			
			
			
