#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)

#fuel column index 8
for line in infile:
	line = line.strip()		#removing the spaces
	my_list = line.split(',')	#splitting the csv file over commas
	#print(my_list)
	if(my_list[0]=='ball'):
		#out='""'
		batsman = my_list[4]
		bowler = my_list[6]
		out = my_list[9]
		#runs = my_list[7]
		#extras = my_list[8]
		#print('%s,%s,%s,%s' % (batsman,bowler,'1', out))
		if(out == ''):
    			print('%s,%s,%s,%s' % (batsman, bowler, '1', '0'))
		elif((out == 'caught') or (out == 'bowled') or (out == 'caught and bowled') or (out =='stumped') or (out =='lbw')):
    			print('%s,%s,%s,%s' % (batsman, bowler, '1', '1'))
		#elif(out == ''):
    		#	print('%s,%s,%s,%s' % (batsman, bowler, '1', '0'))



