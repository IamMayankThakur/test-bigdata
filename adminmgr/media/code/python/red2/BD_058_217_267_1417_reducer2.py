#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

dict1 = {}
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t") #splitting key and value based on tab
	bb = line_val[0].split(",") #splitting batsman and bowler separated by comma
	bb1 = line_val[1].split(",") #splitting runs and extra separated by comma
	key = (bb[1],bb[0]) #key would be the batsman and bowler pair
	run = int(bb1[0]) #converting runs to integer
	extra = int(bb1[1]) #converting extras to integer
	#key, val = line_val[0], line_val[1]
	if key in dict1: #checking if batsman bowler pair in dictionary
		dict1[key][0] += 1 #adding each ball faced by batsman
		dict1[key][1] += run #adding runs scored by batsman against bowler
		dict1[key][1] += extra #adding extras to the runs as well
	else: #if key not in dictionary
		dict1[key] = [1,0] #setting ball as 1
		dict1[key][1] += run #adding the runs conceded on that ball
		dict1[key][1] += extra #adding extras if any     
for key in list(dict1):
	if dict1[key][0] <= 5:
		del dict1[key] #deleting those pairs which have faced less than 5 deliveries
l = sorted(dict1.items()) #sorting in alphabetical order
#print(l)
l1 = sorted(l, key = lambda x: x[1][0]) #sorting based on number of balls
l2 = sorted(l1, key = lambda x: x[1][1], reverse = True) #sorting in descending order based on the number of runs conceded
def custprint(l2):
	for i in l2:
		print('%s,%s,%d,%d' % (str(i[0][0]),str(i[0][1]),i[1][1],i[1][0])) #printing in required format
custprint(l2)
