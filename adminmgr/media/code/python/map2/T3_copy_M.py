#!/usr/bin/python3
"""mapper.py"""
import csv
import sys
with open(sys.argv[1],'r') as infile:
	for line in infile:
		line = line.strip()
		word=line.split(',')
		if word[1]=='venue':
			v= ",".join(word[2:])
			#print(v)
		if word[0]=='ball' :
			print(v+ "*"+word[4],word[7],1,sep="\t")

