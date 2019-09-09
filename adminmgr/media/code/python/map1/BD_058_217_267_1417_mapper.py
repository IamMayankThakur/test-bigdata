#!/usr/bin/python
import sys
import csv
infile = sys.stdin
#next(infile)
count = 0
for line in infile:
	line = line.strip()
	my_list = line.split(',')
	if(my_list[0] == 'ball'): #checking if it is a delivery
		# print('Hello World')
		out = 0
		if(my_list[9] in ["lbw","caught","caught and bowled","bowled","stumped","hit wicket"]): #checking if the ball bowled got a wicket
			out = 1	#setting out as 1 if ball is a wicket
		#print(out)
		key_list = my_list[4]+','+my_list[6]+','+str(out) #setting key as batsman on strike, bowler and if it is a wicket or not
		print('%s\t%s' % (key_list,'1'))  #sending key and '1' from the mapper to the reducer. 1 stands for a delivery bowled
