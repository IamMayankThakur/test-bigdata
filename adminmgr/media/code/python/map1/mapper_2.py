#!/usr/bin/python3
import sys
import csv

content = sys.stdin
count = 0
for line in content:
	line = line.strip()
	li = line.split(",")
	if (len(li) == 11):
		if (li[9] in ['caught', 'lbw', 'bowled', 'caught and bowled', 'hit wicket', 'stumped', 'obstructing the field']):
			wicket = 1
		else:
			wicket = 0
		batsman = li[4]
		bowler = li[6]
		print(wicket,batsman,bowler,1,sep="\t")
