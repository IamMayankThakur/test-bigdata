#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

c=0
d={}
for line in sys.stdin:
	line=line.strip()
	lines=line.split("\t")
	x=lines[0]
	if x not in d:
		d1={}
		d1[lines[1]]=[int(lines[2]),int(lines[3]),int(lines[2])]
		d[x]=d1
	else:
		z=lines[1]
		d1=d[lines[0]]
		if z not in d1:
			d2={}
			d2[lines[1]]=[int(lines[2]),int(lines[3]),int(lines[2])]
			d1.update(d2)
		else:
			d1[z][0]+=int(lines[2])
			d1[z][1]+=int(lines[3])
			d1[z][2]=((d1[z][0])/(d1[z][1]))*100




c=0
l=[]
nd={}
for i in d.items():
	x=i[0]
	z=i[1]
	#print(z)
	for j in z.items():
		#print(j)
		if(j[1][1]>=10):
			#print(j)
			#del((d[x][j[0]]))
			if x not in nd:
				nd[x]={j[0]:j[1]}
			else:
				dodo={j[0]:j[1]}
				nd[x].update(dodo)
	

	
l=[]
for i in nd.items():
	sta=i[0]
	splayer=i[1]
	#print(x)
	'''for j in spalyer.items():
		print(j)'''
	perstad=sorted(splayer.items(),key=lambda x:(-x[1][-1],-x[1][0],x[1]))
	l.append([sta,perstad[0][0]])

		



'''for i in l:
	if '"' in i[0]:
		
		x=i[0][1:]
		i[0]=x
'''

l1=sorted(l,key=lambda x:x[0])
for i in l1:
	print("%s,%s" %(i[0],i[1]) )


	
	

	




