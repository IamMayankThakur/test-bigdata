#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

c_w = 0
c_d = 0
c_bt = ""
c_bo = ""

li=[]
final_list=[]

for line in sys.stdin:
	line = line.strip()
	val = line.split(",")
	bt, bo, w, d = val[0], val[1], val[2], val[3]
	#print('reducee', bat, bowl)
	try:
		w=int(w)
		d=int(d)
	except ValueError:
		continue
	if(c_bt == bt and c_bo == bo):
		c_w += w
		c_d += d
	else:
		if c_bt:
			li.extend([c_bt,c_bo,c_w,c_d])
			#print(li)
			final_list.append(li)			#print(current_bat,',',current_bowl,',',current_wicket,',',current_deli)
		c_bt = bt
		c_bo = bo
		c_w = w
		c_d = d
	li=[]


if c_bt == bt and c_bo == bo:
	#print(current_bat,',',current_bowl,',',current_wicket,',',current_deli)
	li.extend([c_bt,c_bo,c_w,c_d])
	#print(li)
	final_list.append(li)

final_list=sorted(final_list,key=lambda l:(-l[2],l[3],l[0]))
#i=0
for l in final_list:
	if(l[3]>5):
		print(l[0],',',l[1],',',l[2],',',l[3],sep="")
	#i+=1
	#if(i==3):
	#	break
