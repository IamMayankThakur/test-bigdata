#!/usr/bin/python3
#dict1={}
from collections import defaultdict
dict1=defaultdict(list)
import sys
rows=[]
dict2={}

for line in sys.stdin:
	line=line.strip()
	mlist=line.split(',')	
	batsman,bowler,wicket=mlist[0],mlist[1],mlist[2]
	rows.append(mlist)
	key=(batsman,bowler)
	#print(key)
	count=1
	wicket1=0
	#value=[]
	if key in dict1:
		#print(dict1[key])
		new_count=dict1[key][0]
		new_count=new_count+1
		dict1[key][0]=new_count
			
	else:
		dict1[key].append(count)
		dict1[key].append(wicket1)


alldata=[]
for i in range(len(rows)):
	x=rows[i]
	if(x[2]=='1'):
		alldata.append(x)



for i in range(len(alldata)):
		batsman,bowler=alldata[i][0],alldata[i][1]
		key=(batsman,bowler)
		for keys in dict1:
			if key==keys:
				new_count=dict1[keys][1]
				new_count=new_count+1
				dict1[keys][1]=new_count



li=[]
for key in dict1:
	bb=key
	whi=(dict1[key][1])
	rhi=dict1[key][0]
	li.append([bb,whi,rhi])
#print(li)
li.sort()
def Sort(li):			
	for i in range(0,len(li)):
		for j in range(0,(len(li)-i-1)):
			if(li[j][1]<li[j+1][1]):
				tempw=li[j]
				li[j]=li[j+1]
				li[j+1]=tempw
			elif(li[j][1]==li[j+1][1]):
				if(li[j][2]>li[j+1][2]):
					tempr=li[j]
					li[j]=li[j+1]
					li[j+1]=tempr
				elif(li[j][2]==li[j+1][2]):
					if(li[j][0]>li[j+1][0]):
						tempa=li[j]
						li[j]=li[j+1]
						li[j+1]=tempa
	return li
(Sort(li))


for i in range(len(li)):
	if(li[i][2]>5):
		print('%s,%s,%d,%d' % (li[i][0][0],li[i][0][1],li[i][1],li[i][2]))

	
	
	


