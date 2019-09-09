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
	
		if(Batsman!="" and Bowler!=""):
			if(my_list[7]!=0):
				print('%s\t%s\t%d\t%d'%(Bowler,Batsman,1,int(my_list[7])))
			else:
				print('%s\t%s\t%d\t%d'%(Bowler,Batsman,1,int(my_list[8])))
		
	except:
			continue
