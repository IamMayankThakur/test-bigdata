#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
dict1 = {}
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t") #splitting key and value based on tab
	bb = line_val[0].split(",") #splitting the key based on a comma
	key = (bb[0],bb[1]) #Key would be the batsman and the bowler the batsman is facing
	out = int(bb[2]) #converting out to int
	val = line_val[1] #val would hold the value '1' which stands for a delivery bowled
    	#key, val = line_val[0], line_val[1]
	if key in dict1:
		dict1[key][0] += int(val) #counting the number of balls
		if out == 1:
            	#print('out')
			dict1[key][1] += 1 #adding the number of wickets if he got out
	else:
		dict1[key] = [1,0] #setting the number of balls as 1 if he is a new batsman
		if out == 1:
            		#print('out')
			dict1[key][1] += 1  #if batsman is out, adding the wicket           
for key in list(dict1):
	if dict1[key][0] <= 5:
		del dict1[key] #eliminating those pairs which have faced less than 5 deliveries
l = sorted(dict1.items()) #sorting in alphabetical order
l1 = sorted(l, key = lambda x: x[1][0]) #sorting based on number of balls
l2 = sorted(l1, key = lambda x: x[1][1], reverse = True) #sorting output in decreasing number of wickets
def custprint(l2):
	for i in l2:
		print('%s,%s,%d,%d' %(str(i[0][0]),str(i[0][1]),i[1][1],i[1][0])) #printing output in required format
custprint(l2)
