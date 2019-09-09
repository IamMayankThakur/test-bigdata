#!/usr/bin/python3
from __future__ import print_function
import sys
import csv

rows=[]
filename=sys.stdin

for line in filename:
	line=line.strip()
	mylist=line.split(',')
	rows.append(mylist)


for i in range(20,177392):
	if(len(rows[i])==11):
		batsman=rows[i][4]
		bowler=rows[i][6]
		if (rows[i][9] =='""'):
			rows[i][9]=0
			
		elif (rows[i][9]=='run out'):
			rows[i][9]=0
		elif (rows[i][9]=='retired hurt'):
			rows[i][9]=0
			
		else:
			rows[i][9]=1
			
		print("%s,%s,%d" % (batsman,bowler,int(rows[i][9])))

	


