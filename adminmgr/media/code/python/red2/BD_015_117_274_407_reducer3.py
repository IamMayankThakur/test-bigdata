#!/usr/bin/python3

import sys

d= dict()

#function to add each pair of batsmen and bowler to dictionary
def add(bowler, batsman, runs):
	d[(bowler, batsman)] = [int(runs),1]
	return d
	
for lines in sys.stdin:
	#line1 = lines.split()
	lines=lines.strip()
	line = lines.split(",")
	
	if (line[0],line[1]) not in d:
		d = add(line[0],line[1], line[2])
	else:
		d[(line[0],line[1])][1]+=1
		d[(line[0],line[1])][0]+=int(line[2])
		


#removing the pairs with deliveries <= 5
#for key in d:
#	for i in key:
	
		#del d[key]
			
#mutable list			
dlist=list()
for bowl in d:
	temp=list()
	if d[bowl][1]>5:
		for bat in bowl:
			temp.append(bat)
		temp.append(d[bowl][0])
		temp.append(d[bowl][1])
		dlist.append(temp)
	
	
		
#sorting
final=sorted(dlist, key=lambda x: (-x[2], x[3], x[0]))

#final output
for each in final:
	for x in range(0,3):
		print(each[x], end=",")
	print(each[3], end="\n")

#for each in final:
#	print(each)

		
		
