#!/usr/bin/python3
import sys
import csv
#filename='alldata.csv'
rows=[]
filename=sys.stdin

for line in filename:
	line=line.strip()
	mylist=line.split(',')
	rows.append(mylist)

out={}
#print(rows)
#print(len(rows))

for i in range(0,177392):
	if(len(rows[i])==11):
		batsman=rows[i][4]
		bowler=rows[i][6]
		print("%s,%s,%d,%d" % (bowler,batsman,int(rows[i][7]),int(rows[i][8]))) #runs,extras




