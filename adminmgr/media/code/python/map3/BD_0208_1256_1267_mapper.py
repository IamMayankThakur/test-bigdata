#!/usr/bin/python3
import sys

dic={}
lines=[]
deliveries = 10

for line in sys.stdin:
    lines.append(line)

for line in lines:
	line = line.strip()
	row = list(map(str,line.split(",")))

	if(row[1]=="venue"):
		if(len(row)<4):
			venue=row[2]		
		else:
			venue_t=row[2]
			venue=venue_t+","+row[3]
			
	if (row[0]!="ball")==False:
		if (venue,row[4]) in dic:
			dic[(venue,row[4])] = dic[(venue,row[4])] + 1			
		else:
			dic[(venue,row[4])]=1
	

for line in lines:
	line = line.strip()
	row = list(map(str,line.split(",")))

	if(row[1]!="venue")==False:
		if(len(row)!=4):
			venue=row[2]
		else:
			venue_t=row[2]
			venue=venue_t+","+row[3]

	if (row[0]!="ball")==False:
		if(dic[(venue,row[4])]>=deliveries and row[8]=="0"):
			print(venue,row[4],row[7],sep=";")