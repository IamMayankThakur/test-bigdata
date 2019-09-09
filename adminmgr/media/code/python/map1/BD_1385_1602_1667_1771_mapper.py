#!/usr/bin/python3
import sys
import csv
def read_csv():
	for row in sys.stdin:
		row = row.strip()
		string = row.split(',')
		if(len(string)>5 and string[9]>'a' and string[9]<'z' and string[9][0]!='r'):
			string[7] = -5
			print('%s,%s,%d'%(string[4],string[6],int(string[7])))
		elif(len(string)>5):
			print('%s,%s,%d'%(string[4],string[6],int(string[7])))
read_csv()
