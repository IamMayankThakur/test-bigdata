#!/usr/bin/python3
import sys
data=sys.stdin

d={}
rows=[]

'''for row in data:
	row=row.strip()
	row1=list(row.split(","))
	rows.append(row1)'''
for row in data:
	row=row.strip()
	row=list(row.split(","))
	if(row[1]=="venue" and len(row)>3):
		#row1=list(row.split(","))
		#print(row[3])
		row[2]=row[2]+","+row[3]
	rows.append(row)

for row in rows:
	if row[1]=="venue":
		curr_venue=row[2]
		if curr_venue not in d:
			d[row[2]]={}
	elif row[0]=="ball":
		if row[4] not in d[curr_venue]:
			v=d[curr_venue]
			v[row[4]]=[0]*2
			if row[8]=="0":
				v[row[4]][0]=1
				v[row[4]][1]=int(row[7])

		else:
			v=d[curr_venue]
			if row[8]=="0":
				v[row[4]][0]+=1
				v[row[4]][1]+=int(row[7])


for venue in d:
	v=d[venue]
	for player in list(v):
		if v[player][0]<10:
			del v[player]

for venue in d:
	v=d[venue]
	for player in v:
		print(venue,end=":")
		print(player,end=",")
		print(v[player][0],end=",")
		print(v[player][1])

