import sys
import csv

file_contents = sys.stdin

for LINE in file_contents:
	line = LINE.strip()
	LIST = line.split(",")
	if LIST[0] != "ball":
		continue
	if(LIST[9] != '""' and LIST[9] != "run out" and LIST[9] != "retired hurt" and LIST[9] != "obstructing the field"):
		wickets = 1
	else:
		wickets = 0 
	print(LIST[4],LIST[6],wickets,1, sep=",") 		#Batsman, Bowler, No. of wickets, No. of Deliveries
  
	
		
