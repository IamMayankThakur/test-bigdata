#!/usr/bin/python3

import sys

d= dict()

def add(batsman, bowler):
	#the dictionary will have the value tuple (# of wickets, # of balls)
	d[(batsman,bowler)] = [0,1]
	return d
	
for lines in sys.stdin:
		line1 = lines.strip();
		line = 	line1.split(",")
		if (line[0],line[1]) not in d:
			d = add(line[0],line[1])
		else:
			d[(line[0],line[1])][1]+=1

		try:
			if line[2]!='""':
				if line[2]!="run out":
					d[(line[0],line[1])][0]+=1
		except:
			pass

#to get a mutable list to sort
final = list()
for key in d:
	temp = list()
	if d[key][1]>5:
		for i in key:
			temp.append(i)
		temp.append(d[key][0])
		temp.append(d[key][1])
		final.append(temp)


#to sort first based on # of WICKETS, then # of BALLS and lastly BATSMAN'S NAME
final.sort(key= lambda x:(-x[2],x[3],x[0]))

for each in final:
	for x in range(0,3):
		print(each[x],end=",")
	print(each[3],end="\n")
	
