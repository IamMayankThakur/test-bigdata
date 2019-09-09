#!/usr/bin/python3
import sys
import csv
infile = sys.stdin  #taking input from csv file

for line in infile:   #iterating through every line in the file
	line = line.strip()    #removing unecessary blanks
	items = line.split(',')    #spliting the line based on ',' as delimiter
	tag = items[0]      #storing the first column in tag to distinguish between information and ball commentary 
	if(tag == 'ball'):    #if it is a ball commentary
		bowler_batsman = items[6]+','+items[4]   #combining the batsman and bowler name, separated by a ','
		runs = int(items[7])+int(items[8])    #counting the number of runs (in column 8) with extras (in column 9)
		print('%s\t%s\t%s' % (bowler_batsman,'1',runs))     #passing bowler,batsman and the total runs
