#!/usr/bin/python3
import sys
dictionary={}

for line in sys.stdin:
	mapf = map(str,line.split(";"))
	row=list(mapf)
	#score=int(row[2])
	
	if (row[0],row[1]) in dictionary:
		dictionary[(row[0],row[1])][0] = dictionary[(row[0],row[1])][0] + int(row[2])
		dictionary[(row[0],row[1])][1] = dictionary[(row[0],row[1])][1] + 1

	else:
		dictionary[(row[0],row[1])]=[int(row[2]),1]

dic_strike_rate={}


for i in dictionary:
	strike_rate=(dictionary[i][0]*100)/dictionary[i][1]
	if i[0] in dic_strike_rate:
		dic_strike_rate[i[0]].append((i[1],strike_rate,dictionary[i][0]))

	else:
		dic_strike_rate[i[0]]=[]


sortedlist = sorted(dic_strike_rate,key=lambda k:k)
for i in sortedlist:
	#j=sorted(dic_strike_rate[i],key=lambda k:(-k[1],-k[2]))[0]
	print(i,sorted(dic_strike_rate[i],key=lambda k:(-k[1],-k[2]))[0][0],sep=",")

