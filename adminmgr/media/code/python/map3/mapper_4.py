#! /usr/bin/python3
import csv
import sys
infile = sys.stdin
#filename = "alldata.csv"
#fields = [] 
#rows = [] 
venue = ""
for row in infile: 
	row = row.strip()
	my_list = row.split(',')
	if(my_list[0]=="info" and my_list[1] == "venue"):
		venue = ''
		try:
			venue = my_list[2].strip()+","+my_list[3]
		except:
			venue = my_list[2]
	if(my_list[0]=="ball" and int(my_list[8])==0):
		#passing venue, batsman, runs and ball count 
		print(venue,"\t",my_list[4],"\t",my_list[7],"\t",1) 
	
