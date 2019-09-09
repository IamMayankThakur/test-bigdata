#!/usr/bin/python3
#Reducer.py
import sys
from collections import Counter


pair = []
pair_dict={}
count_dict={}
new_dict = {}
list_s = []
#Partitoner
for line in sys.stdin:
	line = line.strip('\n')
	bat_bowl,runs=line.split(':')
	pair.append(bat_bowl)
	
	try:
		if(count_dict[bat_bowl] < -1):
			count_dict[bat_bowl]=1
	except:
		count_dict[bat_bowl]=1
		
	#print(dismissal)
	
	if bat_bowl in pair_dict:
		pair_dict[bat_bowl].append(runs)
		count_dict[bat_bowl] += 1
	else:
		pair_dict[bat_bowl]=[]
		pair_dict[bat_bowl].append(runs)
		
	
#print(pair)
#ctr=0
for key,value in pair_dict.items():
	sum = 0
	for i in value:
		#print(i)
		sum = sum + int(i)
	new_dict[key] = sum
	
for key,value in pair_dict.items():	
	list_s.append([key,new_dict[key],count_dict[key]])



sorted_list = sorted(list_s, key = lambda x : (-x[1],x[2],x[0]))

for i in range(len(sorted_list)):
	if(sorted_list[i][2] > 5):
		print(sorted_list[i][0],sorted_list[i][1],sorted_list[i][2],sep=',')
