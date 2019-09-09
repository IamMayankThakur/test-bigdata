#!/usr/bin/python3
from operator import itemgetter
import sys
import csv

current_count_delivery = 0
current_count_wicket = 0
current_key_batsman = ""
current_key_bowler = ""
Dict = {}
batsman = ""
bowler = ""
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	batsman,bowler,delivery,wicket = line_val[0],line_val[1],line_val[2],line_val[3]
	try:
		count_ball=int(delivery)
		count_wicket=int(wicket)
	except ValueError:
		continue
   
	if (current_key_batsman == batsman and current_key_bowler==bowler):
		current_count_delivery += count_ball
		current_count_wicket += count_wicket 
	else:
		if (current_key_batsman != ""):
			if(current_count_delivery >5):
				Dict[current_key_batsman,current_key_bowler]=current_count_wicket,current_count_delivery
		current_count_wicket = count_wicket
		current_count_delivery = count_ball
		current_key_batsman = batsman
		current_key_bowler = bowler
		
if (current_key_batsman == batsman and current_key_bowler==bowler):
	if(current_count_delivery >5):
		Dict[current_key_batsman,current_key_bowler]=current_count_wicket,current_count_delivery

#Stores dictionary values in list
lst = sorted(Dict.items(), key=itemgetter(1),reverse = True)

#Sorting 
for i in range(0,len(lst)):
	j=i-1
	v = lst[i]
	while((v[1][0]==lst[j][1][0]) and (j>=0)):
		#print(lst[i],'\t',lst[j])
		
		if(v[1][1]<lst[j][1][1]):
				lst[j:j+2] = lst[j+1:j-1:-1]
		
		if(v[1][1]==lst[j][1][1]):
			if(v[0][0]<lst[j][0][0]):
				lst[j:j+2] = lst[j+1:j-1:-1]
			else:
				if(v[0][1]<lst[j][0][1]):
					lst[j:j+2] = lst[j+1:j-1:-1]
		j-=1

#Prints the values			
for t in lst:
	print('%s,%s,%d,%d'%(t[0][0],t[0][1],t[1][0],t[1][1]))



