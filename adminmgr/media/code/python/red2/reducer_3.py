#!/usr/bin/python3
import sys
import csv

d = dict()
mi = []
fi = []
content = sys.stdin
for line in content:
	line.strip()
	li = line.split("\t") 
	li[-1] = li[-1].strip()
	key1 = li[1] + "_" + li[2]
	value1 = [int(li[0]),int(li[3])]
	if key1 not in d.keys():
		d[key1] = value1
	else:
		d[key1][0] += int(li[0])
		d[key1][1] += int(li[3])
for i in d:
	fi = []
	if(d[i][1]>5):
		ki = i.split("_")
		fi.append(ki)
		fi.append(d[i])
		mi.append(fi)
#mi = sorted(mi, key = lambda x : x[0][1])
#mi = sorted(mi, key = lambda x: x[1][1])
#mi = sorted(mi, key = lambda x : x[1][0], reverse = True) 
mi = sorted(mi, key = lambda x : (-x[1][0],x[1][1],x[0][1]))
for i in mi:
	print(i[0][1],i[0][0],i[1][0],i[1][1],sep = ",")
