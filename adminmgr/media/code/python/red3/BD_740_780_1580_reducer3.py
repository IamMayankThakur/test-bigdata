#!/usr/bin/python3
import sys
import csv
infile = sys.stdin

variable1 = None
dict_store = {}

for line in infile:
	line = line.strip()
	line = line.split("\t")
	vne = line[0] #this is used to store the venue name
	strike_rate=float(line[2]) #this is used to store the strike rate
	if vne not in dict_store:
		variable2 = 0
		dict_store[vne] = []
	elif(strike_rate>variable2):
		variable1 = line[1] #this will happen only if the condition is different
		dict_store[vne] = [variable1]
		variable2 = strike_rate #where now the variable2 contains the max strike rate
	elif(strike_rate == variable2): #if the strike rate is similar then the following operation takes place
		variable1 = line[1]
		dict_store[vne].append(variable1)

vne = sorted(list(dict_store.keys())) #function used for sorting

for y in vne:
	print('%s,%s' % (y,dict_store[y][0])) #this is the output of the reducer
