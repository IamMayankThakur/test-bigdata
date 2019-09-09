#!/usr/bin/python3
"""mapper.py"""
import csv
import sys

for line in sys.stdin:
	line = line.strip()
	word=line.split(',')
	if word[1]=='venue':
		v= ",".join(word[2:])
	if word[0]=='ball' and int(word[8]) == 0:
		print(v+ "*"+word[4],word[7],1,sep="\t")
