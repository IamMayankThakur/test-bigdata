#!/usr/bin/python3

import sys
import itertools

def add(x,y,v1,v2):
	#the dictionary will have the value tuple (# of wickets, # of balls)
	final[(x,y)] = [v1,v2]
	return final
	
final=dict()
for line in sys.stdin:
	line=line.strip()
	line=line.split("_")
	
	if (line[0],line[1]) in final:
		final[line[0],line[1]][0]+=int(line[2])
		final[line[0],line[1]][1]+=int(line[3])
	else:
		final = add(line[0],line[1],int(line[2]),int(line[3]))
		
temp=[]
for key,val in final.items():
	runrate=(val[0]/float(val[1]))
	final[key]=(runrate,val[1])

for j in sorted(final.items(),reverse=True,key =lambda x:x[1]):
	temp.append(j)
		
rand=[]

newfinal=dict()
for i in range(len(temp)):
	if(temp[i][0][0] not in rand):
		if(int(temp[i][1][1])>9):
			rand.append(temp[i][0][0])
			newfinal[temp[i][0][0],temp[i][0][1]]=(temp[i][1][0],temp[i][1][1])
	
for key,val in sorted(newfinal.items(),reverse=False,key=lambda x:x[0]):
	print(key[0]+","+key[1])

	

