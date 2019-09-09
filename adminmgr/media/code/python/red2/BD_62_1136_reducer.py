#!/usr/bin/python3
import sys
from operator import itemgetter
dict1={}
cur_run=0
cur_count=0

for line in sys.stdin:
	line=line.strip()
	bt,bo,o,out,r,ex=line.split(",")
	r=int(r)
	ex=int(ex)
	bo=bo.strip()
	bt=bt.strip()
	pair=(bo,bt)

	try:
		o=int(o)	
	except ValueError:
		continue
	
	if pair in dict1.keys():
		dict1[pair]["deliveries"]+=1
		
		dict1[pair]["runs"]+=r
		if ex!=0:
			dict1[pair]["runs"]+=ex
		
	else:
		dict1[pair]={}
		dict1[pair]["deliveries"]=1
		dict1[pair]["runs"]=r+ex
		
final_list=[]
for k in dict1:
	if dict1[k]["deliveries"]>5:
		#print(k,dict1[k]) 
		ll=list(k)
		ll.append(dict1[k]["runs"])
		ll.append(dict1[k]["deliveries"])
		tt=tuple(ll)
		final_list.append(tt)
final_list.sort(key=lambda x:(-x[2],x[3],x[0]+x[1]))


for i in final_list:
	print(",".join(str(j) for j in i))


