#!/usr/bin/python3

import sys

for lines in sys.stdin:
	line = 	lines.split(",")
	try:
			if line[0] == "ball":
				if line[9] != '""':
					if line[9]!= "run out" and line[9]!= "retired hurt":
						print(line[4],line[6],1,sep=",")
					else:
						print(line[4],line[6],0,sep=",")
				else:
					print(line[4],line[6],2,sep=",")

	except:
		pass
					
