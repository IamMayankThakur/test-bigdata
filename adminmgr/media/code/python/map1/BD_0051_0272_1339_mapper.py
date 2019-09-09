#!/usr/bin/python3
import sys
pair={}

for ln in sys.stdin: 
	col=ln.strip()
	col=ln.split(",")
	if(col[0]=="ball"):
		if (col[4],col[6]) in pair:
			pair[(col[4],col[6])]+=1
		elif:
			pair[(col[4],col[6])]=1
			
if(pair[(col[4],col[6])]>=6):
	print(col[6],col[4],int(col[7]),int(col[8]),sep=",")


