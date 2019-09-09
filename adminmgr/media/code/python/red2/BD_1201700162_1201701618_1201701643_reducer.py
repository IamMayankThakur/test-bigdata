#!/usr/bin/python3
import sys
import string

"""class my_dictionary(dict):
	def __init__(self):
		self=dict()
	def add(self,key,value):
		self[key]=value
dict_obj=my_dictionary()"""
dict_obj={}
for line in sys.stdin:
	words=line.split(",")
	value=[]
	
	key=str(words[0])+","+str(words[1])
	#print(key)
	runs=int(words[2])
	deliveries=int(words[3])
	value.append(runs)
	value.append(deliveries)
	if key not in dict_obj.keys():
		dict_obj[key]=value
		#print(dict_obj)
	
	else:
		dict_obj[key][0]+=value[0]
		dict_obj[key][1]+=value[1]

dic = {}
for k, v in dict_obj.items():
	if v[1] > 5:
		dic[k]=v
#print(dic)

items = []
for k, v in dic.items():
	item = []
	item = k.split(",")
	item.append(v[0])
	item.append(v[1])
	items.append(item)
	#print(item)
	#print(type(item[0]))
	#print(type(item[1]))
	#print(type(v[1]))
#print(items)
#print(type(items))


result = sorted(items , key = lambda x:(-x[2],x[3],x[0]))
for value in result:
	#break
	print(value[0]+","+value[1]+",",value[2],",",value[3],sep="")
	#print(type(value[2]))


	
	
	#key = words[0] + "," + words[1]
	
		
