#!/usr/bin/python3
import sys
import csv



for line in sys.stdin:
	line = line.strip()
	line_det = line.split(',')
	if(line_det[0] == 'ball'):
		pair_info = line_det[6]+','+line_det[4]+','+str(int(line_det[7])+int(line_det[8]))
		#print('%s:%s' % (pair_info,'1')) 
		print(pair_info+":1")
