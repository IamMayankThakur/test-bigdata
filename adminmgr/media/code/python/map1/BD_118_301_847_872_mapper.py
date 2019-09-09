#!/usr/bin/python3
import sys
import csv


for line in sys.stdin:
	line = line.strip()
	line_det= line.split(',')
	wicket_types={"obstructing the field":1, "lbw":1,"hit wicket":1 ,"caught":1 ,"bowled":1,"caught and bowled":1,"stumped":1}

	if(line_det[0] == 'ball'):
		if(line_det[9] in wicket_types.keys()):
			bb_pair=line_det[4] + ',' + line_det[6] + ',' + str(wicket_types[line_det[9]])
			#print('%s:%s' % (bb_pair,'1'))
			print(bb_pair + ":" + '1')
		else:
			bb_pair=line_det[4] + ',' + line_det[6] + ',' + str(0)
			#print('%s:%s' % (bb_pair,'1'))
			print(bb_pair + ":"+ "1")







