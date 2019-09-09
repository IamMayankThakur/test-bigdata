#!/usr/bin/python3
import string
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:

    # split the line into words
	words = line.split(",")   
	runs = 0
	if words[0]=="ball":
		runs= int(words[7]) + int(words[8])
     
		mykey=words[4]+"+"+words[6]+"+"+str(runs)
		print (mykey,1,sep="+")
