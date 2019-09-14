#!/usr/bin/python3
import sys
from operator import itemgetter
dict1={}
cur_wick=0
cur_del=0
cur_bat=""
cur_bow=""

for line in sys.stdin:
	line=line.strip()
	bt,bo,w,d=line.split(",")
	
	bo=bo.strip()
	bt=bt.strip()
	pair=(bt,bo)

	try:
		d=int(d)
		w=int(w)	
	except ValueError:
		continue


	if pair not in dict1:
		dict1[pair]={}
		dict1[pair]["wickets"]=w
		dict1[pair]["deliveries"]=1
	else:
		dict1[pair]["wickets"]+=w
		dict1[pair]["deliveries"]+=1





final_list=[]
for k in dict1:
	if dict1[k]["deliveries"]>5:
		#print(k,dict1[k]) 
		ll=list(k)
		ll.append(dict1[k]["wickets"])
		ll.append(dict1[k]["deliveries"])
		tt=tuple(ll)
		final_list.append(tt)
final_list.sort(key=lambda x:(-x[2],x[3],x[0]+x[1]))


for l in final_list:
	print(l[0],',',l[1],',',l[2],',',l[3],sep="")


