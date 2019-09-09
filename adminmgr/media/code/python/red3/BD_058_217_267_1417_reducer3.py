#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

dict1 = {}
li=[]
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t") #splitting line based on comma
	bb = line_val[0] #keeping venue as bb
	val = line_val[1].split(",") #splitting value which is batsman runs based on comma
	bat = val[0] #storing batsman in a variable
	key = bb #setting key as venue
	runs = int(val[1]) #converting runs scored to integer
	
	#key, val = line_val[0], line_val[1]

	if key in dict1: #checking if venue in dictionary
		if bat in dict1[key]: #checking if batsman is present in that venue
			dict1[key][bat][1] += 1 #keeping track of number of balls
			dict1[key][bat][0] += runs #adding runs scored by the batsman at that venue
		else: #if new batsman at the venue
			dict1[key][bat]=[0,1] #setting runs as 0 and balls faced as 1
			dict1[key][bat][0] += runs #keeping track of runs scored by batsman
	else:
		dict1[key] = {} #creating a dictionary if venue doesnt exist 
		dict1[key][bat] = [0,1] #setting runs as 0 and balls faced as 1 
		dict1[key][bat][0] += runs #keeping track of runs scored by batsman         

for key in list(dict1):
	for bat in list(dict1[key]):
		if dict1[key][bat][1] < 10:
			del dict1[key][bat] #eliminating those batsman who have faced less than 10 deliveries


for key in dict1:
	for bat in dict1[key]:
		sr = (dict1[key][bat][0]*100)/dict1[key][bat][1] #computing strike rate for the batsman 
		dict1[key][bat].append(sr) #adding the strike rate to the list of the appropriate batsman
		
for key in dict1:
	venue = key #storing venue
	batsman = list(dict1[key].keys())[0]  #storing batsman
	for bat in dict1[key]:
		if(dict1[key][bat][2] > dict1[key][batsman][2]):
			batsman = bat #finding batsman with highest strike rate
		elif(dict1[key][bat][2] == dict1[key][batsman][2]): #if strike rates are equal
			if(dict1[key][bat][0] > dict1[key][batsman][0]): #comparing based on runs
				batsman = bat #setting new batsman
	li.append((venue,batsman)) #appending venue and batsman with the highest strike rate to a new list	
li.sort()	#sorting list in alphabetical order based on venue
for i in li:
	print(i[0]+','+i[1]) #printing output in the required format



