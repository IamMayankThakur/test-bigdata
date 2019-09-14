#!/usr/bin/python3
import sys
import csv
file = sys.stdin
for line in file:
	line = line.strip()
	my_list  = line.split(',')
	Batsman = my_list[4]
	Bowler = my_list[6]
	Delivery = my_list[7]
	Wicket = my_list[9]
	try:
		if(Batsman!="" and Bowler!=""):
			if(Wicket == "" or Wicket == 'run out' or Wicket == 'retired hurt'):
				print([Batsman,Bowler,0,1])
			else:
				print([Batsman,Bowler,1,1])
		
	except:
			continue
