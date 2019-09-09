#!/usr/bin/python3
import sys
import string

mydict={}
dict_new={}
new_list=[]
for line in sys.stdin:
	words=line.split("+")
	k=words[1]+"+"+words[0]
	try:
		words[3] = int(words[3])
		words[2] = int(words[2])
	except ValueError:
		continue
	if k in mydict.keys():
		mydict[k][0]+=words[2]
		mydict[k][1]+=words[3]
		
	else:
		mydict[k]=[words[2],words[3]]

for key, value in mydict.items():
	if value[1] > 5:
		list1 = key.split("+")
		list1.extend((value[0],value[1]))
		new_list.append(list1)

b = sorted(new_list , key = lambda y:(-y[2],y[3],y[0]))
for final in b:
	print(final[0],final[1],int(final[2]),int(final[3]),sep=",")
		
