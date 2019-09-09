#!/usr/bin/python3
#Reducer.py
import sys
from collections import OrderedDict 
from operator import getitem 
dictred = {}
bats_bowl = []

#Partitoner
for line in sys.stdin:
	line = line.strip()
	line = line.split(',')
	batsman = line[0]
	bowler = line[1]
	out = int(line[2])
	
	key_bb=batsman+','+bowler
	
	if key_bb in dictred:
		dictred[key_bb]['deli'] = dictred[key_bb]['deli'] + 1;
	else:
		d={}
		d['deli']=1;
		d['wick']=0;
		dictred[key_bb]=d

	if out == 1:
		dictred[key_bb]['wick'] += 1



#print(dictred)
sortedalpha_dict = OrderedDict(sorted(dictred.items(), 
       key = lambda x: x[0]))

sortedtie_dict = OrderedDict(sorted(sortedalpha_dict.items(), 
       key = lambda x: getitem(x[1], 'deli'),reverse=False)) 

sorted_dict = OrderedDict(sorted(sortedtie_dict.items(), 
       key = lambda x: getitem(x[1], 'wick'),reverse=True))

'''sortedtie_dict = OrderedDict(sorted(dictred.items(), 
       key = lambda x: getitem(x[1], 'deli'),reverse=False))''' 
		
for i in sorted_dict.keys():
	if(sorted_dict[i]['deli'] >5):
		bats_bowl = i.split(',')
		print('%s,%s,%d,%d' % (bats_bowl[0], bats_bowl[1], sorted_dict[i]['wick'], sorted_dict[i]['deli']))
