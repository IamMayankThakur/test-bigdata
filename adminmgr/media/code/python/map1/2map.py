import sys
import csv

for line in sys.stdin:
	line = line.strip()
	list_ = line.split(",")

	try:
		batsman = list_[4]
		bowler = list_[6]
		if(list_[9] == "caught" or list_[9] == "bowled" or list_[9] == "lbw" or  list_[9] == "caught and bowled" or  list_[9] == "stumped" ):
			print('%s\t%s\t%d\t%d'% (batsman,bowler,1,1))
			
		else:
			print('%s\t%s\t%d\t%d'% (batsman,bowler,0,1))
	except:
		continue

