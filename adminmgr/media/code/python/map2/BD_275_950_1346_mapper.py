#!/usr/bin/python3
"""mapper.py task2"""
import csv
import sys

for line in sys.stdin:
	line = line.strip()
	word=line.split(',')
	if word[0]=='ball':
		print(word[4]+ "*"+word[6],int(word[7])+int(word[8]),1, sep='\t')

