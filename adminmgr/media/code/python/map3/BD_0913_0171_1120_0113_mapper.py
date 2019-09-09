#!/usr/bin/python3
import sys
import csv

file_contents = sys.stdin
o = []
venue = ""
for line in file_contents:
	line = line.strip()
	LINE = line.split(",")
	o.append(LINE)
l = len(o)
i = 0
while (i < l):
	if  o[i][0]=="info":
		while(i < l and o[i][0]=="info"):
			if (len(o[i]) == 3 and o[i][1]=="venue"):
				venue = o[i][2]
			else:
				if (o[i][1] == "venue"):
					venue = o[i][2] + "," + o[i][3]
			i += 1
		while(i < l and len(o[i]) == 11):
			if o[i][8] == "0":			#Removing the extras
				print(venue,o[i][4],int(o[i][7]),1,sep=":")
			i += 1
		continue
	i += 1

