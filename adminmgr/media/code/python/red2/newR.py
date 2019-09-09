#!/usr/bin/python3
import sys

pairs=dict()
for record in sys.stdin:
    record=eval(record)
    if((record[0],record[1]) in pairs):
        pairs[(record[0],record[1])][0]+=record[2]
        pairs[(record[0],record[1])][1]+=record[3]
    else:
        pairs[(record[0],record[1])]=[record[2],record[3]]

list_pairs=[]
for pair in pairs:
    if(pairs[pair][1]>5):		#remove cases with deliveries<5
        list_pairs.append(pair+tuple(pairs[pair]))

#sort 1) desc wickets 2) asc deliveries 3) batsman name
list_pairs.sort(key= lambda r: (-r[2],r[3],r[0]))

for i in list_pairs:
	#print(i)
    print(i[0],i[1],i[2],i[3],sep=",")
