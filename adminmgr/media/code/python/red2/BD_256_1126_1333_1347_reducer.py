#!/usr/bin/python3
import sys
from collections import OrderedDict
from operator import getitem

d = {}


for line in sys.stdin:
	val  = 0
	line = line.strip()
	line = line.split(',')
	batsman = line[0]
	bowler = line[1]
	run = int(line[2])
	extra = int(line[3])
	val = run + extra

	key_b = bowler +','+ batsman

	if key_b in d:
		d[key_b]['deli'] = d[key_b]['deli'] + 1
		d[key_b]['runn'] = d[key_b]['runn'] + val
	else:
		d1 = {}
		d1['deli'] = 1
		d1['runn'] = val
		d[key_b] = d1

	#print(d)

	'''if(key_b[0] == 'A Mishra'):
		print(d[key_b])'''
'''
for key1 in d:
	for key2 in d:
		if(d[key1]['runn']==d[key2]['runn'] and d[key1]['deli']==d[key2]['deli']):
			sorted_dict1 = OrderedDict(sorted(d.items(),key = lambda k:k[0]))
			sorted_dict2 = OrderedDict(sorted(sorted_dict1.items(),key = lambda k: getitem(k[1], 'deli'),reverse=False)) 
			sorted_dict = OrderedDict(sorted(sorted_dict2.items(),key = lambda k: getitem(k[1], 'runn'),reverse=True))
'''

sorted_dict1 = OrderedDict(sorted(d.items(),key = lambda k:(k[0])))

sorted_dict2 = OrderedDict(sorted(sorted_dict1.items(),key = lambda k: getitem(k[1], 'deli'),reverse=False)) 

sorted_dict = OrderedDict(sorted(sorted_dict2.items(),key = lambda k: getitem(k[1], 'runn'),reverse=True))

for i in sorted_dict.keys():
	if(sorted_dict[i]['deli'] > 5):
		batsmanbowler = i.split(',')
		print('%s,%s,%d,%d' % (batsmanbowler[0],batsmanbowler[1],sorted_dict[i]['runn'],sorted_dict[i]['deli']))



