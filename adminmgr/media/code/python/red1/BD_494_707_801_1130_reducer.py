#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

results = {}
for line in sys.stdin:
	line = line.strip()
	temp = line.split("=")
	tup1 = temp[0].split(",")
	key1=tup1[0]
	key2=tup1[1]
	final_key= (key1,key2)
	wicket = tup1[2]
	wicket=int(wicket)

	if final_key in results:
		results[final_key][1]+= 1
		if(wicket):
			results[final_key][0]+=1
	else:
		results[final_key]=[0,1]
		if(wicket):
			results[final_key][0] += 1

for key in list(results):
	if results[key][1] <= 5:
		del results[key]

l = sorted(results.items())
l1 = sorted(l, key = lambda x: x[1][1])
l2 = sorted(l1, key = lambda x: x[1][0], reverse = True)
def custprint(l2):
    for i in l2:
        print(str(i[0][0])+","+str(i[0][1])+","+str(i[1][0])+","+str(i[1][1]))
custprint(l2)

#sorted_result = sorted(sorted(sorted(results.items()), key = lambda x: x[1][1]), key = lambda x: x[1][0], reverse = True)
#for pairs in sorted_result:
#	print ("%s,%s,%d,%d" % (str(pairs[0][0]),str(pairs[0][1]),(pairs[1][0]),(pairs[1][1])))