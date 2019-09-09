#!/usr/bin/python
import sys
import csv
infile = sys.stdin
#next(infile)
count = 0
for line in infile:
	line = line.strip()
	my_list = line.split(',') #splitting line based on comma
	if(my_list[0] == 'ball'): #checking if the line is a delivery
		# print('Hello World')
		#print(out)
		key_list = my_list[4]+','+my_list[6] #keeping key as a batsman bowler pair
		val_list = my_list[7]+','+my_list[8] #keeping value as runs scored by on strike batsman and extras 
		print('%s\t%s' % (key_list,val_list))  #sending key and value to reducer
