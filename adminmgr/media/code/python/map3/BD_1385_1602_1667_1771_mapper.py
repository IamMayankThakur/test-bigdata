#!/usr/bin/python3
import sys
import csv

def read_csv():
	for row in sys.stdin:
		row = row.strip()
		string=row.split(',')
		
		
		if(len(string)<5):
			if(string[1]=='venue'):
				if(len(string)>3):
					stadium=string[2][1:]+","+string[3][:len(string[3])-1]
					
				else:
					stadium=string[2]
		elif(len(string)>5):
			print('%s|%s|%d|%d'%(stadium,string[4],int(string[7]),int(string[8])))
			
read_csv()
	
	
