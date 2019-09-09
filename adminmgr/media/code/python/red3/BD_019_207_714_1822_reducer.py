#!/usr/bin/python3
import sys
import itertools
final=dict()
for line in sys.stdin:
	line=line.strip()
	linearray=line.split("*")
	
	key1,key2,val1,val2=linearray[0],linearray[1],int(linearray[2]),int(linearray[3])
	if((key1,key2) in final):
		d=final[key1,key2]
		final[key1,key2]=(val1+d[0],val2+d[1])
	else:
		final[key1,key2]=(val1,val2)
old=[]
for k,v in final.items():
	#print(k)
	runrate=(v[0]/float(v[1]))
	final[k]=(runrate,v[1])
some=dict()

for j in sorted(final.items(),reverse=True,key =lambda x:x[1]):
	old.append(j)
#print(some)		
ano=[]
#print(old)
one=[]

rr=dict()
for i in range(len(old)):
	if(old[i][0][0] not in ano):
		if(int(old[i][1][1])>9):
			ano.append(old[i][0][0])
			rr[old[i][0][0],old[i][0][1]]=(old[i][1][0],old[i][1][1])
	
for k,v in sorted(rr.items(),reverse=False,key=lambda x:x[0]):
	print(k[0]+","+k[1])

