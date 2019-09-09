#!/usr/bin/python3
import sys
import string

for line in sys.stdin:
	words=line.split(",")
	runs=0
	if words[0]=='ball':
		runs=int(words[7])+int(words[8])
		key=words[6]+","+words[4]
		print(key,runs,1,sep=",")
		

		
