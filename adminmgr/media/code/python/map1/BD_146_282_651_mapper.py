#!/usr/bin/env python3
import sys
import csv
lista = ['bowled' , 'caught' , 'lbw' , 'stumped' , 'caught and bowled' , 'hit wicket' , 'obstructing the field']

for line in sys.stdin:
	line = line.strip()
	list_a = line.split(',')
	#print(list_a[0])
	
	if(list_a[0] == 'ball'):
		out = list_a[9]
		if out in lista:
			print('%s,%s\t%s\t%s' % (list_a[10],list_a[6],'1','1'))
		else:
			print('%s,%s\t%s\t%s' % (list_a[4],list_a[6],'0','1'))
		
		
