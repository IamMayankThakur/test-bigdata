#!/usr/bin/python3
import csv
from operator import itemgetter #CONSTRUCT CALLABLE FOR ITERABLE INPUT
import itertools
import sys

d_set = dict()	#Dictionary to read dataset
i = 0
for new in sys.stdin:
	new = new.strip() #REMOVE SPACES IN STRING  BOTH LEADING AND TRAILING
	new_x = new.split(",")
	k1,k2,k3,k4 = new_x[0], new_x[1],int(newarray[3])

	if((k1,k2) in d_set.keys()):
		d = d_set[k1,k2]
		d_set[k1,k2] = (k3 + d[0] , k4 + d[1]) #ASSIGNING AND INCREASING KEYS WITH RUN VALUES PER BATSMAN
	else:
		d_set[k1,k2] = (k3,k4)



for a,v in list(d_set.items()): #RETURN LIST WITH DICT KEYS AND VALUES
	v1,v2 = v[0],v[1]
	
	if(v[1] < 6):
		del d_set[a]
previous = []			#OLD LIST IS EMPTY
l = sorted(d_set.items(),key = lambda x:(-x[1][0], x[1][1])) #SORTING LIST TO OBTAIN IN REQUIRED ORDER


for a in range(len(1)):
	print(l[a][0][0] + "," + l[a][0][1]+","str(l[a][1][0]) + "," + l[a][1][1])
	#PRINT VALUES AND KEY FOR EACH ITEM IN SORTED LIST	
