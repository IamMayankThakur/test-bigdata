#!/usr/bin/python3
import sys
import csv
import operator
context=sys.stdin
venue=""
batsman=""
runs=0
dict={}
for line in context:
	line=line.strip()
	line=line.replace("(","")
	line=line.replace(")","")
	line=line.split("\t")
	venue=line[0]
	batsman=line[1]
	runs=int(line[2])
	if((venue,batsman) not in dict):
		dict.setdefault((venue,batsman),[])
		dict[(venue,batsman)].append(runs)
	else:
		dict[(venue,batsman)].append(runs)
final={}
for k in dict:
	runs=0
	ball=0
	for balls in dict[k]:
		ball=ball+1
	if(ball>=10):
		for run in dict[k]:
			runs=run+runs
		final.setdefault(k,[])
		strike=(float(runs)/float(ball))*100
		final[k].append(strike)
		final[k].append(runs)

dest={}
for key in final:
	k=key[0]
	if k not in dest:
		dest.setdefault(k,[])
		dest[k].append(key[1])
		dest[k].append(final[key][0])
		dest[k].append(final[key][1])
	else:
		if(final[key][0]>dest[k][1]):
			dest[k][0]=key[1]
			dest[k][1]=final[key][0]
			dest[k][2]=final[key][1]
		elif(final[key][0]==dest[k][1]):
			if(final[key][1]>dest[k][2]):
				dest[k][0]=key[1]
				dest[k][1]=final[key][0]
				dest[k][2]=final[key][1]
dest=sorted(dest.items(),key=operator.itemgetter(0))
for key in dest:
	venue=key[0]
	batsman=key[1][0]
	#venue=venue.replace("\'","")
	#batsman=batsman.replace('"',"")
	#batsman=batsman.replace("\'","")
	print(venue+","+batsman)

		
