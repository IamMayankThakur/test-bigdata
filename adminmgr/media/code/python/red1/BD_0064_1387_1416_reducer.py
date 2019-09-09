#!/usr/bin/python3
import sys
#f=open('op.txt','r')
reader=sys.stdin
task1={}
for row in reader:
	row=row.strip()
	players,num=row.split(':')
	p=tuple(players.split(","))
	n=list(num.split(","))
	n[0]=int(n[0])
	n[1]=int(n[1])
	#print("player:",players)
	#print("num:",type(num))
	if int(n[1])>5:
		task1[p]=n
#print(final)
#without sorting:
modlist=[] #based on number of wickets
modlist=sorted(task1.items(),reverse=True,key=lambda kv:kv[1][0])
#print(modlist[0])
for x in range(0,len(modlist)):
#x is each Tuple in list ((b,b),(w,r))
#(x[1][1]) is no. of deliveries
	y=0
	while y<(len(modlist)-x-1):
		if(modlist[y+1][1][0])==(modlist[y][1][0]):
			if(modlist[y][1][1]>modlist[y+1][1][1]):
									modlist[y],modlist[y+1]=modlist[y+1],modlist[y]
		y=y+1

for x in range(0,len(modlist)):
	for y in range(0,x):
		if(modlist[y+1][1][0]==modlist[y][1][0]):
			if(modlist[y+1][1][1]==modlist[y][1][1]):
				if(modlist[y][0][0]>modlist[y+1][0][0]):
											modlist[y],modlist[y+1]=modlist[y+1],modlist[y]
#print("swapped")

for v in modlist:
	print(v[0][0],end=",")
	print(v[0][1],end=",")
	print(v[1][0],end=",")
	print(v[1][1])

