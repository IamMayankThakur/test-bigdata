#!/usr/bin/python3
import sys
import csv

content = sys.stdin
mi = []
venue = ""
ki = []
for line in content:
	line = line.strip()
	li = line.split(",")
	mi.append(li)
length = len(mi)
i = 0
while (i < length):
	if (len(mi[i]) == 3):
		while(i < length and (len(mi[i]) == 3 or len(mi[i]) == 4)):
			if (len(mi[i]) == 3):
				if (mi[i][1] == 'venue'):
					venue = mi[i][2]
			else:
				if (mi[i][1] == 'venue'):
					venue = mi[i][2] + "," + mi[i][3]
			i += 1
		while(i < length and len(mi[i]) == 11):
			if mi[i][8] == '0':
				print(venue,mi[i][4],int(mi[i][7]),1,sep="\t")
			i += 1
	else:
		i += 1
