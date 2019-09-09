#!/usr/bin/python3
import sys
import csv
content = sys.stdin
count = 0
for line in content:
	line = line.strip()
	li = line.split(",")
	if (len(li) == 11):
		batsman = li[4]
		bowler = li[6]
		print(int(li[7])+int(li[8]),batsman,bowler,1,sep="\t")
		
