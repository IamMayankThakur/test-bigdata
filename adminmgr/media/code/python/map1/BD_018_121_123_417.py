#!/usr/bin/python3
import sys
import csv
file = sys.stdin
for line in file:
	line = line.strip()
	my_list  = line.split(',')
	
	try:
		Batsman = my_list[4]
		Bowler = my_list[6]
		Delivery = my_list[7]
		Wicket = my_list[9]
		if(Batsman!="" and Bowler!=""):
			if(Wicket == "" or Wicket == 'run out' or Wicket == 'retired hurt'):
				print('%s\t%s\t%d\t%d'%(Batsman,Bowler,1,0))
			else:
				print('%s\t%s\t%d\t%d'%(Batsman,Bowler,1,1))
		
	except:
			continue
