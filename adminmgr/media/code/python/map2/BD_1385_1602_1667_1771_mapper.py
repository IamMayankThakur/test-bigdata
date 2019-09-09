#!/usr/bin/python3 
import sys
import csv
def read_csv():
	for row in sys.stdin:
		row = row.strip()
		string = row.split(',')
		if (len(string)>5 and string[9]>'a' and string[9]<'z' and string[9][0]!='r'):
			print('%s,%s,%d,%d' % (string[6],string[4],int(string[7]),int(string[8])))
		elif(len(string)>5):
			print('%s,%s,%d,%d' % (string[6],string[4],int(string[7]),int(string[8])))
			
read_csv()		
