#!/usr/bin/python3
"""mapper.py"""
import csv
import sys
for line in sys.stdin:
	line = line.strip()
	word=line.split(',')
	if(word[0]=='ball'):
		print(word[4]+ "*"+word[6],word[9],1, sep = "\t")

