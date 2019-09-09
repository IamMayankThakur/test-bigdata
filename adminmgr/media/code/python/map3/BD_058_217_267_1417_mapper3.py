#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)
venue = ''
for line in infile:
	line = line.strip()
	my_list = line.split(',') #splitting the line based on comma
	if(my_list[0] == 'info' and my_list[1] == 'venue'): #checking if line has info and if it does checking if that info is a venue about the match
		if(len(my_list)==4): #if length of that list is 4, there is something after the comma to be considered
			venue = my_list[2]+','+my_list[3] #adding the appropriate fields
		else:
			venue = my_list[2] #if there is nothing after the comma, just adding the venue
	elif(my_list[0] == "ball"): #if the line contains info of ball bowled
		if(int(my_list[8]) != 0): #checking if extras are there
			continue
		else:
			key_list = venue #setting key as venue
			val_list = my_list[4]+','+my_list[7] #setting value as batsman and runs scored by batsman
			print('%s\t%s' % (key_list,val_list))	
		  
