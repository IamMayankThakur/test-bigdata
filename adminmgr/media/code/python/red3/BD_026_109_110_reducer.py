#!/usr/bin/python3
import sys
import csv

d = {}
content = sys.stdin
mi = []
for line in content:
	line = line.strip()
	li = line.split("\t")
	key = li[0] + "_" + li[1]
	#print(len(li),type(li))
	#print(key)
	if key not in d:
		ki = []
		ki.append(int(li[2]))
		ki.append(1)
		d[key] = ki
	else:
		d[key][0] += int(li[2])
		d[key][1] += 1
#print(d)
for i in d:
	fi = []
	if d[i][1] >= 10:
		ki = i.split("_")
		#if (ki[0][0] == '"'):
			#ki[0] = ki[0][1:]
		fi.append(ki)
		fi.append(d[i])
		mi.append(fi)
#mi = sorted(mi, key = lambda x : x[1][0], reverse = True)
mi = sorted(mi, key = lambda x : (x[0][0],-(x[1][0]/x[1][1]),-x[1][0]))
#mi = sorted(mi, key = lambda x : x[0][0])

alpha = []
for i in mi:
	alpha.append(i[0][0])
#print(set(alpha))
#print(len(set(alpha)))

stadium = mi[0][0]
#print(mi)
#print(mi[0][0],mi[0][1],sep=",")
for i in mi:
	if stadium == i[0][0]:
		continue
	else:
		print(i[0][0],i[0][1],sep=",")
		stadium = i[0][0]
#print(mi)
