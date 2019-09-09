#!/usr/bin/python3
import sys
import csv
context=sys.stdin
d=dict()
for line in context:
	line.strip()
	li=line.split("\t")
	li[3]=li[3].strip()
	key=li[0]+","+li[1]
	val=[int(li[2]) , int(li[3])]
	if key not in d.keys():
		d[key]=val
	else:
		d[key][0]=d[key][0]+int(li[2])
		d[key][1]=d[key][1]+int(li[3])
a=[]
for keys in d:
	li_alpha=[]
	if(d[keys][1]>5):
		li_beta=keys.split(",")
		li_alpha.append(li_beta)
		li_alpha.append(d[keys])
		a.append(li_alpha)
a=sorted(a,key=lambda x: x[0][1])
a=sorted(a,key=lambda x: x[1][1])
a=sorted(a,key=lambda x: x[1][0],reverse=True)
#a1=sorted(a,key=lambda x:(x[0][1],x[1][1],-x[1][0]))
for i in a:
	#print(,sep=",")
	print("%s,%s,%d,%d" %(i[0][1],i[0][0],i[1][0],i[1][1]))