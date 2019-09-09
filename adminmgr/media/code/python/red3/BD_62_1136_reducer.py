#!/usr/bin/python3 
import sys
from operator import itemgetter
dict1={}
cur_run=0
cur_count=0

for line in sys.stdin:
	line=line.strip()
	venue,bt,r,d=line.split("$")
	r=int(r)
	d=int(d)
	pair=(venue,bt)
	try:
		d=int(d)
	except ValueError:
		continue
	if pair in dict1.keys():
		dict1[pair]["deliveries"]+=1
		dict1[pair]["runs"]+=r
	else:
		dict1[pair]={}
		dict1[pair]["deliveries"]=1
		dict1[pair]["runs"]=r
final_list=[]
distinct_venues=[]
distinct_venues=set(distinct_venues)

for k in dict1:
	distinct_venues.add(k[0])
	if dict1[k]["deliveries"]>=10:
		dict1[k]["strike_rate"]=(100*dict1[k]["runs"])/dict1[k]["deliveries"]
		ll=list(k)
		ll.append(dict1[k]["deliveries"])
		ll.append(dict1[k]["strike_rate"])
		tt=tuple(ll)
		final_list.append(tt)


final_list.sort(key=lambda x:x[3])
length=len(final_list)

output=[]
for d in distinct_venues:
	max_strikerate=0.0
	batsman=''
	z=0
	while z<length:
		if final_list[z][0]==d:
			strikerate=float(final_list[z][3])
			if strikerate > max_strikerate:
				batsman=final_list[z][1]
				max_strikerate=strikerate
		z+=1
	output.append((d,batsman))
output.sort(key=lambda x:x[0]+x[1])
for i in output:
	print(",".join(str(j) for j in i))



