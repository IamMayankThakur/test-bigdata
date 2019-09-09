#!/usr/bin/python
import csv
from operator import itemgetter
import sys
import csv

maplist=[]
#data =open("C:\\Users\\Abishek\\Documents\\SEM_5\\4_BD\\alldata.csv")


#content = data.readlines()


#for line in content:
	#print(line)
	
for line in sys.stdin:
	line = line.strip()
	line_split = line.split(",")
	if(len(line_split)>7):
		key = line_split[6]+","+line_split[4]
		
		val=int(line_split[7])+ int(line_split[8])
		maplist.append((key+","+str(val)))
