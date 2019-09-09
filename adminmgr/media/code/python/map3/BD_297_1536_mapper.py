#!/usr/bin/python3
import sys
import csv
infile = sys.stdin   #taking input from csv file

for line in infile:   #iterating through every line in the file
	line = line.strip()   #removing unecessary blanks
	items = line.split(',')   #spliting the line based on ',' as delimiter
	tag = items[0]     #storing the first column in tag to distinguish between information and ball commentary
	ven = items[1]     #storing the second column to check for the venue
	if(tag == 'info' and ven == 'venue'):  #if it is a information line and venue row
		if len(items)>3:   #to check if the venue has any comma within the name	  
			venue = items[2]+','+items[3]     #if yes, pass the combination of the split strings (whole name)
		else:
			venue = items[2]	#if not, just pass the name directly
	if(tag == 'ball' and items[8]=='0'):    #if it is a ball commentary and that ball is not an extra (9th column is 0)
		batsman = items[4]    #extract the batsman name
		runs = int(items[7])  #extract the runs scored in that ball
		print('%s\t%s\t%s' % (venue+'::'+batsman,'1',runs))   #pass a combination of venue::batsman and runs in that ball  
