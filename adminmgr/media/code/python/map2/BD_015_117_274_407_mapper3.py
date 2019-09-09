#!/usr/bin/python3

import sys

for lines in sys.stdin:
	line = lines.split(",")
	try:
		if line[0] == "ball":
			print(line[6], line[4], int(line[7])+int(line[8]), sep=",")
	
	except:
		pass
