import sys
import csv

file_contents = sys.stdin
for line in file_contents:
	LINE = line.strip()
	LIST = LINE.split(",")
	if(LIST[0] == "info" and LIST[1]=="venue"):
		venue = ""
		v = LIST[2:]
		for i in v:
			venue+=i
			venue+=","
	if(LIST[0] == "ball"):
		print (venue, LIST[4], LIST[7], sep=":")

