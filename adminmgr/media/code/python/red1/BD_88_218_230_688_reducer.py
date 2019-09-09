#!/usr/bin/python3
import sys
from collections import Counter

pair = []
delivery=[]
wicket_dict={}

#Partitoner
for line in sys.stdin:
	line = line.strip('\n')
	batsman,bowler,wicket=line.split(',')
	wicket=int(wicket)
	key_bat_bowl=batsman+','+bowler
	delivery.append(key_bat_bowl)
	if key_bat_bowl in wicket_dict:
		wicket_dict[key_bat_bowl]+=wicket
	else:
		wicket_dict[key_bat_bowl]=[]
		wicket_dict[key_bat_bowl]=wicket

#dict with number of deliveries as value
final_delivery_count=Counter(tuple(delivery))
output=[]
for key,value in wicket_dict.items():
	if(final_delivery_count[key]>5):
		output.append([key,value,final_delivery_count[key]])

#alphabetical= sorted(output,key=lambda x:x[0].lower());
ans = sorted(output,key=lambda x: (x[1],-x[2]),reverse=True);
for value in ans:
	print(value[0],value[1],value[2],sep=',');

	
