#!/usr/bin/python3
#Reducer.py
import sys

dic = {}

#Partitoner
for fil in sys.stdin:
	line = fil.strip()
	list1= line.split(',')
	if(len(list1)>4):
		batsman=list1[0]
		runs=list1[1]
		venue=list1[2]+','+list1[3]
		deli=list1[4]
	else:
		batsman=list1[0]
		runs=list1[1]
		venue=list1[2]
		deli=list1[3]
	d={}
	d['run']=int(runs)
	d['de']=int(deli)

	if venue in dic:
		if batsman in dic[venue]:
			dic[venue][batsman]['run']=dic[venue][batsman]['run']+ int(runs)
			dic[venue][batsman]['de']=dic[venue][batsman]['de']+ int(deli)
		else:
			dic[venue][batsman]=d
		
	else:
		dic[venue] = {}
		dic[venue][batsman]=d
final_dict={}

#Reducer
for k in dic.keys():
	if k not in final_dict:
		final_dict[k]={}
	for j in dic[k].keys():
		dic[k][j]['srate']=((dic[k][j]['run']*100)/(dic[k][j]['de']))
		if(dic[k][j]['de']>=10):
			final_dict[k][j]={}
			final_dict[k][j]['srate']=dic[k][j]['srate']
			final_dict[k][j]['runs']=dic[k][j]['run']

final_list=[]
for k in final_dict.keys():
	l=[]
	m=[]
	for j in final_dict[k].keys():
		l.append((j,final_dict[k][j]['srate'],final_dict[k][j]['runs']))
	f=sorted(l, key = lambda x:x[2] ,reverse=True)
	m=sorted(f, key = lambda x: x[1],reverse=True)
	
	for i in m:
		final_list.append((k,i[0]))
		break
final_list1=sorted(final_list,key = lambda x:x[0])
for h in final_list1:
	print('%s,%s' % (h[0],h[1]))
