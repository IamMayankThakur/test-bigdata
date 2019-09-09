#!/usr/bin/python3
import sys
d={}
for line in sys.stdin:
	line = line.strip()
	line = line.split(",")
	if(line[1]=="venue"):
		venue=line[2]
		if (len(line)== 4):
			venue=venue+","+line[3]	
	if(line[0]=="ball" and int(line[8],10)== 0):
		batsmen=line[4]
		batv=venue+"/"+batsmen
		runs=int(line[7],10)
		if batv not in d:
			d[batv]=[]
			d[batv].append(runs)
		else:
			d[batv].append(runs)
new={}
for key,value in d.items():
	new[key]=[]
	total=0
	for val in value:
		total+=val
	new[key].append(total)
	new[key].append(len(value))
for sr in new:
	print("%s\t%s"%(sr,str(new[sr][0])+"/"+str(new[sr][1])))

	
		

