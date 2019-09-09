import sys
import csv

file_contents = sys.stdin
for line in file_contents:
	LINE = line.strip()
	LIST = LINE.split(",")
	if(LIST[0] == "ball"):
		print(LIST[6],LIST[4],LIST[7],1,sep=",")

