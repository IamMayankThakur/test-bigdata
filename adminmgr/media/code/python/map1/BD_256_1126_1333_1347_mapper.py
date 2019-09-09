#!/usr/bin/env python3
#"""mapper.py"""

import sys

out = 1
dictmapper={}

# input comes from STDIN (standard input)
for line in sys.stdin:
	line = line.strip()
	line = line.split(",")

	if line[0] == "ball":
		batsman = line[4]
		bowler = line[6]
		outc = line[9]
		key = batsman+','+bowler
	

		if(outc =='caught' or outc =='bowled' or outc =='caught and bowled' or outc =='lbw' or outc == 'stumped' or outc =='hit wicket' or outc == 'obstructing the field'):
			dictmapper[key] = 1
		else:
			dictmapper[key] = 0
		
		print('%s,%s' % (key, dictmapper[key]))     
		

		


        

