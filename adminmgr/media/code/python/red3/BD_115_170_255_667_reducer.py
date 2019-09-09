#!/usr/bin/env python3
import csv
from operator import itemgetter
import sys

venues = {}
count = 0
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	venue = line_val[0]
	batsmen = line_val[1]
	strike = float(line_val[2])
	score = int(line_val[3])
	ball = int(line_val[4])
	
	if(venue not in venues.keys()):
		venues[venue] = [batsmen,strike,score,ball]
	else:
		if strike > venues[venue][1]:
			venues[venue] = [batsmen,strike,score,ball]		
		elif strike == venues[venue][1]:
			if score > venues[venue][2]:
				venues[venue] = [batsmen,strike,score,ball]
	
for key in venues:
	print('%s,%s' % (key,venues[key][0]))
