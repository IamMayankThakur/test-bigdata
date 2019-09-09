#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

current_run = 0
current_deli = 0
current_bat = ""
current_bowl = ""

li=[]
tot_list=[]

for line in sys.stdin:
	line = line.strip()
	line_val = line.split(",")
	bowl, bat, run, deli = line_val[0], line_val[1], line_val[2], line_val[3]
	#print('reducee', bat, bowl)
	try:
		run=int(run)
		deli=int(deli)
	except ValueError:
		continue
	if(current_bat == bat and current_bowl == bowl):
		current_run += run
		current_deli += deli
	else:
		if current_bat:
			li.extend([current_bowl,current_bat,current_run,current_deli])
			#print(li)
			tot_list.append(li)			#print(current_bat,',',current_bowl,',',current_wicket,',',current_deli)
		current_bat = bat
		current_bowl = bowl
		current_run = run
		current_deli = deli
	li=[]


if current_bat == bat and current_bowl == bowl:
	#print(current_bat,',',current_bowl,',',current_wicket,',',current_deli)
	li.extend([current_bowl,current_bat,current_run,current_deli])
	#print(li)
	tot_list.append(li)

tot_list=sorted(tot_list,key=lambda l:(-l[2],l[3]))
#i=0
for l in tot_list:
	if(l[3]>5):
		print(l[0],',',l[1],',',l[2],',',l[3],sep="")
	#i+=1
	#if(i==3):
	#	break
