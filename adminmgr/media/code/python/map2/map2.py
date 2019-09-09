#!/usr/bin/python3
import sys

from operator import itemgetter
for line in sys.stdin:
	line = line.strip()
	line_split = line.split(",")
	if(len(line_split)>7):
		key = line_split[6]+","+line_split[4]
		
		val=int(line_split[7])+ int(line_split[8])
		print(key+","+str(val))
