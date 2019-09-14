#!/usr/bin/python3
import sys
dataset = syst.stdin #FUNCTION CALL TO READ LINES IN DATASET

for x in dataset:
	set=x.split(",")  #SPLIT COLUMN DATA
	if(len(set)>9):
		if(set[9]!='run out' and set[9]!='""' and set[9]!= 'retired hurt'):	
			print(set[4],set[6],1,1,sep=",") #CASES NOT UNDER WICKETS
		else:
			print('%s ,%s ,%d ,%d '%set[4],set[6],0,1))
