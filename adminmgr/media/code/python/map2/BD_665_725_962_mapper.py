#!/usr/bin/python3
import sys
import csv

for line in sys.stdin:
	line = line.strip()
	list_ = line.split(",")

	try:
		batsman = list_[4]
		bowler = list_[6]
		run = list_[7]
		extra = list_[8]
		total = int(run) + int(extra)
		print('%s\t%s\t%d\t%d'%(bowler,batsman,int(total),1))	
		
	except:
		continue

