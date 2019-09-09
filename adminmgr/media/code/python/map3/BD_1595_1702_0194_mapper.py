#!/usr/bin/python3
# mapper function used for wickets taken
import sys
import csv  
for line in sys.stdin:
	line = line.strip()
	words = line.split(',') # split line into parts
	
	'''	
	if(len(words) > 5 and (words[9] == ' ' or words[9] == "run out" or words[9] == "retired hurt")):
		#key1 = (words[4],words[6])
		#values1 = (words[7],0);
		print('%s,%s,%d' % (words[4],words[6],int(words[7])))
	if(len(words) > 5 and (words[9] != ' ' and words[9] != "run out" and words[9] != "retired hurt")):
		words[7] = -1
		#key = (words[4],words[6])
		#values = (words[7],0);
		print('%s,%s,%d' % (words[4],words[6],int(words[7])))
	'''
	if(len(words)<5):
		if(words[1]=='venue'):
			if(len(words)>3):			
				stadium=words[2][1:]+","+words[3][:len(words[3])-1]
				
			else:
				stadium=words[2]
			#print(stadium)
	elif(len(words)>5):
		print('%s|%s|%d|%d' % (stadium,words[4],int(words[7]),int(words[8])))
