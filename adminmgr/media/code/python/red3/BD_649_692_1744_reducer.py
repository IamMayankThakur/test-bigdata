#!/usr/bin/python3
from operator import itemgetter
import sys
def strikerate(x,y):
  return ((float(x)/float(y)) * 100)
total = {}
dict_ven = {}
for line in sys.stdin:
    line = line.strip()
    key, value = line.split(':')

    # reformatting the input given
    key_attr = key.split('~')
    venue = key_attr[0]
    batsman = key_attr[1]
    if(venue in dict_ven):
        dict_ven[venue] = dict_ven[venue] + 1
    else:
      dict_ven.update({venue: 1})  
   
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
# print(len(result))
# removing all cases where n of deliveries is less than 10
reduced = {}
temp = [{i:total[i]} if total[i][0] >= 10 else None for i in total]


for j in temp:
	if(j != None):
		for i in j:
			#print(i)
			j[i] = strikerate(j[i][1],j[i][0])
			if(list(i)[0] not in reduced):
				reduced.update({list(i)[0]: {list(i)[1]: j[i]}})
			else:
				reduced[list(i)[0]].update({list(i)[1]: j[i]})
for i in sorted(reduced.keys()):
    res =sorted(reduced[i].items(), key=itemgetter(1), reverse=True)
    print('%s,%s' % (str(i),str(list(res[0])[0])))
    
