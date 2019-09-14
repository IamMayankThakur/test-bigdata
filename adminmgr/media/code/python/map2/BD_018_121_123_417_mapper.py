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
	
	try:
		if(Batsman!="" and Bowler!=""):
				print('%s\t%s\t%d\t%d'%(Bowler,Batsman,int(my_list[7])+int(my_list[8]),1))
	except:
			continue
