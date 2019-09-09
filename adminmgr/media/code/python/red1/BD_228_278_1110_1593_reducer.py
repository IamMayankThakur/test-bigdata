#!/usr/bin/python3
import sys
dic={}
for line in sys.stdin:
	row=list(map(str,line.split(",")))
	batsmanout=row[3][:-1]
	#print(row,batsmanout)
	if (row[0],row[1]) not in dic:
		#print(row[2])
		if(row[2]!='nan' and row[2]!='run out' and row[2]!='retired hurt'):
			dic[(row[0],row[1])]=[1,1]
			#print("hey")
		else:
			dic[(row[0],row[1])]=[0,1]

	else:
		dic[(row[0],row[1])][1]+=1
		if(row[2]!='nan' and row[2]!='run out' and row[2]!='retired hurt'):
			dic[(row[0],row[1])][0]+=1

#print(dic)

for i in sorted(dic,key=lambda k:(-dic[k][0],dic[k][1],k[0])):
	print(i[0],i[1],dic[i][0],dic[i][1],sep=",")