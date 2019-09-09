#!/usr/bin/python3
import sys
d={}
lis=[]
for i in sys.stdin:
	lis.append(i)
	entry=list(map(str,i.split(",")))
	if(entry[1]=="venue"):
		if(len(entry)!=4):
			if(entry[2][-1]=="\n"):
				venue=entry[2][:-1]
			else:
				venue=entry[2]
		else:
			venue_t=entry[2]
			if(entry[3][-1]=="\n"):
				venue=venue_t+","+entry[3][:-1]
			else:
				venue=venue_t+","+entry[3]
			
	if(entry[0]=="ball"):
		if (venue,entry[4]) not in d:
			d[(venue,entry[4])]=1
		else:
			d[(venue,entry[4])]+=1

for i in lis:
	entry=list(map(str,i.split(",")))
	if(entry[1]=="venue"):
		if(len(entry)!=4):
			if(entry[2][-1]=="\n"):
				venue=entry[2][:-1]
			else:
				venue=entry[2]
		else:
			venue_t=entry[2]
			if(entry[3][-1]=="\n"):
				venue=venue_t+","+entry[3][:-1]
			else:
				venue=venue_t+","+entry[3]
	if(entry[0]=="ball"):
		if(d[(venue,entry[4])]>=10 and entry[8]=="0"):
			print(venue,entry[4],entry[7],sep="|")
