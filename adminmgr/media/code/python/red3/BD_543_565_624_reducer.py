#!/usr/bin/python3
import sys
# f=open("reduce3.csv","w+")
di={}
for y in sys.stdin:
	Record=list(map(str,y.split(",")))
	if(len(Record)>3):
		Record=[Record[0]+","+Record[1],Record[2],Record[3]]
	s=int(Record[2][:-1])
	if (Record[0],Record[1]) not in di:
		di[(Record[0],Record[1])]=[s,1]
	else:
		di[(Record[0],Record[1])][0]+=s
		di[(Record[0],Record[1])][1]+=1
dsr={}
for i in di:
	sr=(di[i][0]*100)/di[i][1]
	if i[0] not in dsr:
		dsr[i[0]]=[]
	else:
		dsr[i[0]].append((i[1],sr,di[i][0]))
for i in sorted(dsr,key=lambda x:x):
	j=sorted(dsr[i],key=lambda x:(-x[1],-x[2]))[0]
	print(i,j[0],sep=",")
	# f.write(i+","+j[0]+"\n")
