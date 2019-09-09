#!/usr/bin/python3
import sys
import string
dict_obj={}
for line in sys.stdin:
	words=line.split(",")
	batsman=words[0]
	bowler=words[1]
	key=batsman+","+bowler
	wickets=int(words[2])
	deliveries=int(words[3])
	if key not in dict_obj.keys():
		dict_obj[key]=[wickets,deliveries]
	else:
		dict_obj[key][0]+=wickets
		dict_obj[key][1]+=deliveries
dict_1={}
for k,v in dict_obj.items():
	if v[1]>5:
		dict_1[k]=v


items = []
for k, v in dict_1.items():
	item = []
	item = k.split(",")
	item.append(v[0])
	item.append(v[1])
	items.append(item)


result = sorted(items , key = lambda x:(-x[2],x[3],x[0],x[1]))
for value in result:
	#break
	print(value[0]+","+value[1]+",",value[2],",",value[3],sep="")
	#print(type(value[2]))





	
	
