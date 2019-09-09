#!/usr/bin/python3
import sys
import csv
infile = sys.stdin  #taking input from csv file

for line in infile:   #iterating through every line in the file
	line = line.strip()    #removing unecessary blanks 
	items = line.split(',')    #spliting the line based on ',' as delimiter
	tag = items[0]      #storing the first column in tag to distinguish between information and ball commentary 
	if(tag == 'ball'):    #if it is a ball commentary
		batsman_bowler = items[4]+','+items[6]   #combining the batsman and bowler name, separated by a ','
		print('%s\t%s\t%s' % (batsman_bowler,'1',items[9]))     #passing batsman,bowler and the 10th column (which has information about wickets)
