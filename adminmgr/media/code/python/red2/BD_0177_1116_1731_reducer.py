#!/usr/bin/python3
import sys
current_word = None
current_wickets = 0
current_deliveries=0
word = None
dict1={}
list2=[]
list3=[]
for line in sys.stdin:
	line = line.strip()
	word,wickets,deliveries = line.split("_")
	word=word.strip()
	wickets = int(wickets)
	deliveries=int(deliveries)
	if current_word == word:
		current_wickets += wickets
		current_deliveries += deliveries
	else:
		if current_word:
			if(current_deliveries>5):
				list3.append([[current_word],[current_wickets,current_deliveries]])
		current_wickets = wickets
		current_deliveries=deliveries
		current_word = word
if current_word == word and current_deliveries>5:	
	list3.append([[current_word],[current_wickets,current_deliveries]])

list3=sorted(list3,key=lambda x:(-x[1][0],x[1][1],x[0][0]))

for i in list3:
	print(i[0][0],i[1][0],i[1][1],sep = ",")
