#!/usr/bin/python3
import string
import sys
import csv
#csv_input=sys.stdin
for line in sys.stdin:
	words=line.split(",")
	if(words[1]=="venue"):
		if len(words)>3:
			venue=words[2]+","+words[3]
		else:
			venue=words[2]
		venue=venue.replace("\n","")
		#print(venue)
	if(len(words)==11 and words[8]=='0'):
		mykey=venue+"+"+words[4]+"+"+words[7]
		print(mykey,1,sep="+")
		