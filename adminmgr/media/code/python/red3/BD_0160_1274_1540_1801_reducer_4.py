#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

current_venue = ""
current_bat = ""
current_runs=0
current_deli=0

li=[]
tot_list=[]

for line in sys.stdin:
	line = line.strip()
	line_val = line.split(",")
	if(len(line_val)==5):
		venue, bat, runs, deli = line_val[0]+','+line_val[1], line_val[2], line_val[3], line_val[4]
	else:	
		venue, bat, runs, deli = line_val[0], line_val[1], line_val[2], line_val[3]
	#print(venue, bat, runs,deli)
	try:
		runs=int(runs)
		deli=int(deli)
	except ValueError:
		continue
	if(current_venue == venue and current_bat == bat):
		current_runs += runs
		current_deli += deli
	else:
		if current_venue:
			li.extend([current_venue,current_bat,current_runs,current_deli])
			#print(li)
			tot_list.append(li)			
		current_venue = venue
		current_bat = bat
		current_runs = runs
		current_deli = deli
	li=[]

if current_venue == venue and current_bat == bat:
	li.extend([current_venue,current_bat,current_runs,current_deli])
	#print(li)
	tot_list.append(li)

tot_list.sort()

final_list=list(filter(lambda x:x[3]>=10,tot_list))

current_venue = final_list[0][0]
hero=final_list[0][1]
max_strike=(100*final_list[0][2])/final_list[0][3]
max_runs=final_list[0][2]
for l in final_list[1:]:
	strike=(100*l[2])/l[3]
	if(current_venue == l[0] and strike>max_strike):
		hero=l[1]
		max_runs=l[2]
		max_strike=strike
	elif(current_venue == l[0] and strike==max_strike and l[2]>max_runs):
		hero=l[1]
		max_runs=l[2]
	elif(l[0]!=current_venue):
		print(current_venue,',',hero,sep="")		
		current_venue=l[0]
		hero=l[1]
		max_runs=l[2]
		max_strike=strike
print(current_venue,',',hero,sep="")	
	#print(l[0],',',l[1],',',l[2],',',l[3],strike)



