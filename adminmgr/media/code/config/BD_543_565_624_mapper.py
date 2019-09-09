#!/usr/bin/python3
import sys
# f=open("map.csv","w+")
from operator import itemgetter
for line in sys.stdin:
	line = line.strip()
	line_split = line.split(",")
	if(len(line_split)>7):
		key = line_split[4]+","+line_split[6]
		if(line_split[9]=='\"\"' or line_split[9]=="run out" or line_split[9]=="retired hurt"):
			val=0
		else:
			val=1
		# f.write((key+","+str(val)+"\n"))
		print((key+","+str(val)))