#!/usr/bin/python3
#import pandas as pd
'''my_cols=['info','ball','innings','batting_team','batsman1','batsman2','bowler','batsman1_score','extras','wicket','batsman_out']
data = pd.read_csv("alldata.csv",names=my_cols, skipinitialspace=True, engine='python')'''
dic={}
'''n=0

for index,row in data.iterrows():
	if(row['info']=="ball"):
		if (row['batsman1'],row['bowler']) not in dic:
			dic[(row['batsman1'],row['bowler'])]=1
		if(dic[(row['batsman1'],row['bowler'])]!=6):
			dic[(row['batsman1'],row['bowler'])]+=1

for index,row in data.iterrows():
	if(row['info']=="ball"):
		if(dic[(row['batsman1'],row['bowler'])]==6):
			n+=1

print(n)'''
import sys
li=[]
for line in sys.stdin: 
	#print(line)
	li.append(line)
	row=list(map(str,line.split(",")))
	if(row[0]=="ball"):
		if (row[4],row[6]) not in dic:
			dic[(row[4],row[6])]=1
		elif(dic[(row[4],row[6])]!=6):
			dic[(row[4],row[6])]+=1


for line in li:
	row=list(map(str,line.split(",")))
	if(row[0]=="ball"):
		if(dic[(row[4],row[6])]==6):
			print(row[6],row[4],int(row[7]),int(row[8]),sep=",")