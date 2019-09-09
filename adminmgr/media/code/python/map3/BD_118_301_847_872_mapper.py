#!/usr/bin/python3
import sys
import csv


venue = ''
for line in sys.stdin:
	line = line.strip()
	line_det = line.split(',')
	if(line_det[0] == 'info' and line_det[1] == 'venue'):
		#print(line_det)
		if(len(line_det)>=4):
		#if(line_det[3]!=''):
			venue = line_det[2]+","+line_det[3]		
		else:
			venue=line_det[2]
			
			
			
	elif(line_det[0] == "ball"):
		if(int(line_det[8])!=0):
			continue
		key_venue = venue	
		vals = line_det[4]+','+line_det[7]
		#print('%s:%s' % (key_venue,vals)) 
		print(key_venue + ":" + vals)
