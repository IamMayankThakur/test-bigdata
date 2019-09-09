#!/usr/bin/python3
import sys
import csv
inf = sys.stdin
vne = None #to hold the venue name
dict_store = {}


for line in inf:
	line = line.strip()
	line = line.split(",")
	if(line[1] == 'venue'): #this is to check and see if there is venue in the column #this is when the first column has info
		if(len(line) == 4):
			vne = line[2]+','+line[3] #merge both the venue name and the place
		else:
			vne = line[2]	#only includes the venue name
		if(vne not in dict_store):
			dict_store[vne] = {} #if venue isnt present then store empty value
	elif(line[0] == 'ball'): #checking the first column
		variable1 = line[4] 
		if variable1 not in dict_store[vne]:
			dict_store[vne][variable1] = [0,0]
		runs = int(line[7]) #storing the runs
		dict_store[vne][variable1][0] += runs #calculating the total runs
		if(int(line[8]) == 0 or (int(line[8]) == 1 and runs>0)):
			dict_store[vne][variable1][1] += 1

for y in dict_store:
	for variable2 in dict_store[y]:
		if(dict_store[y][variable2][1] >= 10): #to check if the delivaries is more than 10
			strike_rate = dict_store[y][variable2][0]/dict_store[y][variable2][1] #this is used to calculate the strike rate
			print('{}\t{}\t{}'.format(y,variable2,strike_rate)) #output to the reducer
