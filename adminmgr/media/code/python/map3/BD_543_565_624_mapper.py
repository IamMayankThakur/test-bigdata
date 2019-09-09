#!/usr/bin/python3
import sys
# fp=open("map3.csv","w+")
d={}
l=[]
for x in sys.stdin:
	l.append(x)
	Record=list(map(str,x.split(",")))
	if(Record[1]=="venue"):
		if(len(Record)>3):	
			if(Record[2][0]=='\"\"' ):
				v1=Record[2][:-1]+","+Record[3].strip()				
			else:
				v1=Record[2]+","+Record[3].strip()		
		if(Record[2][-1]=="\n"):
			v=Record[2][:-1]
		else:
			v=Record[2].strip()
		if(len(Record)>3):
			v=v1.strip()
	if(Record[0]=="ball"):
		if (v,Record[4]) not in d:
			d[(v,Record[4])]=1
		else:
			d[(v,Record[4])]+=1
for x in l:
	# print(x)
	Record=list(map(str,x.split(",")))
	if(Record[1]=="venue"):
		if(len(Record)>3):	
			if(Record[2][0]=='\"\"' ):
				v1=Record[2][:-1]+","+Record[3].strip()
				
			else:
				v1=Record[2]+","+Record[3].strip()

		if(Record[2][-1]=="\n"):
			v=Record[2][:-1]
		else:
			v=Record[2].strip()
		if(len(Record)>3):
			v=v1.strip()
	if(Record[0]=="ball"):
		if(d[(v,Record[4])]>=10):
			# print(v,Record[4],Record[7],sep=",")
			# print(v+","+str(Record[4])+","+str(Record[7]))
			if(Record[8]=='0'):
				print(v,Record[4],Record[7],sep=",")
				# fp.write(v+","+Record[4]+","+Record[7]+"\n")
# fp.close()