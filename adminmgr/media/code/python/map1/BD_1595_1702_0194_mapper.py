#!/usr/bin/python3
# mapper function used for wickets taken
import sys
import csv  
for line in sys.stdin:
	line = line.strip()
	words = line.split(',') # split line into parts

	if(len(words) > 5 and words[9]>'a' and words[9]<'z' and words[9][0]!='r'):#((words[9]) and words[9] != "run out" and words[9] != "retired hurt")):
		words[7] = -1	
		print('%s,%s,%d' % (words[4],words[6],int(words[7])))
	elif(len(words)>5):
		print('%s,%s,%d' % (words[4],words[6],int(words[7])))
