#!usr/bin/python
import csv 
from operator import itemgetter
import sys
final = {}
balls = 0
runs = 0
batter_bowler = ['','']
if not sys.warnoptions:
	import warnings
	warnings.simplefilter('ignore')
	

for line in sys.stdin:
	line = line.strip()
	line_val = line.split(',')
	key = (line_val[0],line_val[1])
	val = (int(line_val[2]),int(line_val[3]))
	try:
		deliveries = val[0]
		run = val[1]
	except ValueError:
		continue
	global balls
	global runs 
	global batter_bowler
	if batter_bowler[0] == key[0] and batter_bowler[1] == key[1]:
		runs = runs + run
		balls = balls + deliveries
	else:
		if(batter_bowler[0]!='' and batter_bowler[1]!='' and balls > 5):
			final[(batter_bowler[0],batter_bowler[1])]=[runs,balls]
		balls = 1
		runs = 0
		batter_bowler[0] = key[0]
		batter_bowler[1] = key[1]
if(batter_bowler[0]!='' and batter_bowler[1]!='' and balls > 5):
			final[(batter_bowler[0],batter_bowler[1])]=[runs,balls]

print(final)
	
