#!/usr/bin/python3
import string
import sys
import csv
csv_input=sys.stdin
for line in csv.reader(iter(csv_input.readline,'')):
	words=line
	#words=line.split(",")
	if(words[1]=="venue"):
		venue=words[2]
		venue=venue.replace("\n","")
		#print(venue)
	if(len(words)==11 and words[8]=='0'):
		if(words[3]!=""):
			mykey=venue+"+"+words[4]+"+"+words[7]
			print(mykey,1,sep="+")
