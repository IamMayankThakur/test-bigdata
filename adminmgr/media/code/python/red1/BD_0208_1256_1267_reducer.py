#!/usr/bin/python3
import sys
dictionary={}
#										b1			b2	out		bowler
# example output line from mapper -Sachin Baby,B Kumar,run out,CJ Jordan

for line in sys.stdin:
	line = line.strip()
	row=list(map(str,line.split(",")))

	if (row[0],row[1]) in dictionary:
		dictionary[(row[0],row[1])][1]+=1
		if(row[2]=='run out' or row[2]=='nan' or row[2]=='retired hurt'):
			pass
		else:	
			dictionary[(row[0],row[1])][0] = dictionary[(row[0],row[1])][0] + 1

	else:
					
		if(row[2]=='run out' or row[2]=='nan' or row[2]=='retired hurt'):
			dictionary[(row[0],row[1])]=[0,1]

		else:
			dictionary[(row[0],row[1])]=[1,1]	# no of wickets stores in 0th pos and in 1th pos is the number of deliveries



 
sortedlist = sorted(dictionary,key=lambda k:(-dictionary[k][0],dictionary[k][1],k[0]))

for i in sortedlist:
	print(i[0],i[1],dictionary[i][0],dictionary[i][1],sep=",")