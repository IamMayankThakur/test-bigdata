import sys
from collections import OrderedDict
from operator import getitem

d = {}
for line in sys.stdin:
	line = line.strip()
	line = line.split(',')
	batsman = line[0]
	bowler = line[1]
	run = int(line[2])

	key_b = batsman+','+ bowler

	if key_b in d:
		d[key_b]['deli'] = d[key_b]['deli'] + 1
		d[key_b]['runs'] = d[key_b]['runs'] + run
	else:
		d1 = {}
		d1['deli'] = 1
		d1['runs'] = run
		d[key_b] = d1

#print(d1)

sorted_dict1 = OrderedDict(sorted(d.items(),key = lambda k: k[0].lower()))

sorted_dict2 = OrderedDict(sorted(sorted_dict1.items(),key = lambda k: getitem(k[1], 'deli'),reverse=False)) 

sorted_dict = OrderedDict(sorted(sorted_dict2.items(),key = lambda k: getitem(k[1], 'runs'),reverse=True))

for i in sorted_dict.keys():
	if(sorted_dict[i]['deli'] > 5):
		batsmanbowler = i.split(',')
		print('%s,%s,%d,%d' % (batsmanbowler[0], batsmanbowler[1],int(batsmanbowler[i]['runs']), int(batsmanbowler[i]['deli'])))

