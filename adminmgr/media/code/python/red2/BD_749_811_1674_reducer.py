#!/usr/bin/python3

from collections import defaultdict
dict1=defaultdict(list)
import sys
rows=[]
dict2={}
#print("\nreducer\n");
for line in sys.stdin:
		line=line.strip()
		#print(line)
		list1=line.split(',')
		rows.append(list1)

#print(rows)		

#print(dict1)
"""
for i in range(len(rows)):
	key=(rows[i][0],rows[i][1])
	dict2[(rows[i][0],rows[i][1])]=[]
"""
#dict_runs={}
temp_dict={}

for k in range(len(rows)):
	(batsman,bowler)=(rows[k][1],rows[k][0])
	k1=(bowler,batsman)
	#print(k1)
	runs=0
	
	if k1 in temp_dict:
		pass
	else:
		temp_dict[k1]=[]
		temp_dict[k1].append(runs)

"""
for key,value in temp_dict.items():
	print(key,value)
"""


for j in range(len(rows)):
	(batsman,bowler,runs,extras)=(rows[j][1],rows[j][0],rows[j][2],rows[j][3])
	runs=int(runs)
	extras=int(extras)
	key2=(bowler,batsman)
	#print(key2)
	count=0

	if key2 in temp_dict:
		prev_runs=int(temp_dict[key2][0]) 
		new_runs=runs+extras+prev_runs
		temp_dict[key2][0]=new_runs
		#temp_dict[key2].append(count)

for k in temp_dict:
	temp_dict[k].append(count)
"""	
for key,value in temp_dict.items():
	print(key,value)

for key,value in temp_dict.items():
	print(key,value[0],value[1])
"""


for i in range(len(rows)):
	(batsman,bowler)=(rows[i][1],rows[i][0])
	key=(bowler,batsman)
	count=1
	

	if key in temp_dict:
		new_count=temp_dict[key][1]
		new_count=new_count+1
		temp_dict[key][1]=new_count

#print(temp_dict)		

"""
for key,value in temp_dict.items():
	print('%s,%s,%d,%d'% (key[0],key[1],value[0],value[1])) #bowler,batsman,runs,deliveries
	

for key,value in temp_dict.items():
	print(key,value)
	
"""

#print(sorted(temp_dict)) #to sort by bowler's name
#print(sorted(temp_dict.items(), key = lambda x : x[1],reverse=True)) #to sort by runs

#list1=sorted(temp_dict.items(), key = lambda x : x[1],reverse=True)
#print(list1[10][0][0])
#print(list1)
"""
for l in list1:
	print(l)
"""
"""
for i in range(len(list1) and i!=len(list1)):
	if((list1[i][1][0])==(list1[i+1][1][0])):       #if runs are equal
		#print("hello")
		if((list1[i][1][1])<(list1[i+1][1][1])): 
			temp=()
			temp=list1[i]
			list1[i]=list1[i+1]
			list1[i+1]=temp
		elif((list1[i][1][1])==(list1[i+1][1][1])):
			if((list1[i][0][0])>(list1[i+1][0][0])):
				temp1=()
				temp1=list1[i]
				list1[i]=list1[i+1]
				list1[i+1]=temp1
			
			

#print(list1)


for j in range(len(list1)):
	print('%s,%s,%d,%d' % (list1[j][0][0],list1[j][0][1],list1[j][1][0],list1[j][1][1]))

"""
li=[]
for key in temp_dict:
	bb=key
	deliveries=(temp_dict[key][1])
	runs=(temp_dict[key][0])
	#print(runs,deliveries)
	li.append([bb,runs,deliveries])
#print(li)
li.sort()
def Sort(li):			
	for i in range(0,len(li)):
		for j in range(0,(len(li)-i-1)):
			if(li[j][1]<li[j+1][1]):
				tempr=li[j]
				li[j]=li[j+1]
				li[j+1]=tempr
			elif(li[j][1]==li[j+1][1]):
				if(li[j][2]>li[j+1][2]):
					tempd=li[j]
					li[j]=li[j+1]
					li[j+1]=tempd
				elif(li[j][2]==li[j+1][2]):
					if(li[j][0]>li[j+1][0]):
						tempa=li[j]
						li[j]=li[j+1]
						li[j+1]=tempa
	return li
(Sort(li))

for i in range(len(li)):
	if(li[i][2] > 5):
		print(("%s,%s,%d,%d") % (li[i][0][0],li[i][0][1],li[i][1],li[i][2]))





