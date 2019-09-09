#!/usr/bin/python3
import sys
import csv
L = []
for line in sys.stdin:
	line = line.strip()
	list_ = line.split(",")
	
	try:
		batsman = list_[4]
		bowler = list_[6]
				
		if(list_[9] == "caught" or list_[9] == "bowled" or list_[9] == "lbw" or  list_[9] == "caught and bowled" or  list_[9] == "stumped" or list_[9] == "obstructing the field" or list_[9] == "hit wicket"):
			print('%s\t%s\t%d\t%d'%(batsman,bowler,1,1))
			#L.append((batsman,bowler,1,1))
		else:
			print('%s\t%s\t%d\t%d'%(batsman,bowler,0,1))
			#L.append((batsman,bowler,0,1))	
	except:
		continue

'''for i in sorted(L , key = lambda x : (x[0],x[1])):
	print('%s\t%s\t%d\t%d'% (i[0],i[1],i[2],i[3]))'''

