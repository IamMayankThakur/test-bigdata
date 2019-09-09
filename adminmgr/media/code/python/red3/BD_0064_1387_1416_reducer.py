#!/usr/bin/python3
import sys
lines=[]
result={}
line1=[]
for line in sys.stdin:
	line1=[]
	line=line.strip()
	stad,num=line.split(":")
	num=list(num.split(","))
	line1.append(stad)
	line1.append(num[0])
	line1.append(num[1])
	line1.append(num[2])
	line1[2]=int(line1[2])
	line1[3]=int(line1[3])
	lines.append(line1)

i=0	
final={}
while i < len(lines):
	venue=lines[i][0]
	final[venue]=lines[i][1]
	rr=lines[i][3]*100/lines[i][2]
	j=1
	while j+i<len(lines) and lines[j+i][0]==venue:
		if rr<(lines[j+i][3]*100/lines[j+i][2]):
			final[venue]=lines[j+i][1]
			rr= lines[j+i][3]*100/lines[i+j][2]
		elif rr==(lines[j+i][3]*100/lines[j+i][2]):
			if lines[i+j][2]>lines[i][2]:
				final[venue]=lines[i+j][1]
				rr=lines[i+j][3]*100/lines[i+j][2]
			else:
				final[venue]=lines[i][1]
				rr=lines[i][3]*100/lines[i][2]
		j+=1
	i+=j
for key in sorted(final.items()):
	print(key[0],end=",")
	print(key[1])
