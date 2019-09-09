#!/usr/bin/python3
#import pandas as pd
'''my_cols=['info','ball','innings','batting_team','batsman1','batsman2','bowler','batsman1_score','extras','wicket','batsman_out']
data = pd.read_csv("alldata.csv",names=my_cols, skipinitialspace=True, engine='python')'''
pair={}

import sys
li=[]
for ln in sys.stdin: 
	
	li.append(ln)
	col=list(map(str,ln.split(",")))
	if(col[0]=="ball"):
		if (col[4],col[6]) in dic:
			pair[(col[4],col[6])]+=1
		elif:
			pair[(pair[4],pair[6])]=1


for ln in li:
	col=list(map(str,ln.split(",")))
	if(col[0]=="ball"):
		if(pair[(col[4],col[6])]==6):
			print(col[6],col[4],int(col[7]),int(col[8]),sep=",")