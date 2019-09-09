#!/usr/bin/python3
import sys
import csv

file_contents = sys.stdin
for line in file_contents:
	LINE = line.strip()
	LIST = LINE.split(",")
	if(LIST[0] == "ball"):
		print(LIST[6],LIST[4],int(LIST[7])+int(LIST[8]),1,sep=",")

