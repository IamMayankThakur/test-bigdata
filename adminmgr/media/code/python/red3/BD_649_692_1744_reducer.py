#!/usr/bin/env python
from operator import itemgetter
import sys
def strikerate(x,y):
  return ((float(x)/float(y)) * 100)
  
total = dict()
dict_ven = dict()
for l in sys.stdin:
    l = l.strip()
    key, value = l.split(':')
    attribute = key.split('~')
    venue = attribute[0]
    batsman = attribute[1]
    if(venue not in dict_ven):
        dict_ven.update({venue: 1})  
    else:
        dict_ven[venue] = dict_ven[venue] + 1
        
    key = (venue, batsman)

    value = value.strip()
    value = value.split(',')
    deliveries = int(value[0][1:])
    runs = int(value[1][:-1])

    if(key not in total.keys()):
        total.update({key: [deliveries, runs]})
    else:
        total[key][0] = total[key][0] + deliveries
        total[key][1] = total[key][1] + runs
# print(len(total))
# removing all cases where n of deliveries is less than 10
reduced = {}
temp = [{i:total[i]} if total[i][0] >= 10 else None for i in total]

#print(temp)

for j in temp:
	#if(j != None):
		for i in j:
			#print(i)
			j[i] = strikerate(j[i][1],j[i][0])
			if(list(i)[0] not in reduced):
				reduced.update({list(i)[0]: {list(i)[1]: j[i]}})
			else:
				reduced[list(i)[0]].update({list(i)[1]: j[i]})
for i in reduced:
    res = sorted(reduced[i].items(), key=itemgetter(1), reverse=True)
    print(str(i), str(list(res[0])[0]))
 
   

