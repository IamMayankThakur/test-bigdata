#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
x=''
c=0
for line in infile:
	line = line.strip()
	#lines= line.split(',')
	'''if(lines[0]=='info'):
		if(lines[1]=='venue'):
			x=lines[2]'''

	lines = line.split(",")
	if (lines[0]=="info" and lines[1]=="venue"):
		if(lines[2][0]=='"'):
			x=lines[2]+","+lines[3]
		else:
			x=lines[2]

	if(lines[0]=='ball' and lines[8]=='0'):
		print("%s\t%s\t%s\t%d" % (x,lines[4],lines[7],1))
	
	#c=c+1
	#if(c==400):
	#	quit()
		

