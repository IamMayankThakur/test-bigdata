#!/usr/bin/python3
import sys
from itertools import groupby
from operator import itemgetter

newdict={}

def read_mapper():
	for row in sys.stdin:
		row=row.strip()
		stadium,batsman,run,extras=row.split('|')
		key=stadium
		
		if(key not in newdict):
			newdict[key]={}
		if(batsman not in newdict[key]):
			newdict[key][batsman]=[0,0,0]
		if(extras=='0'):
			newdict[key][batsman][0]+=int(run)
			newdict[key][batsman][1]+=1
			newdict[key][batsman][2]=newdict[key][batsman][0]/newdict[key][batsman][1]
			
read_mapper()
output={}
def strike(output):
	for i in newdict:
		if(i not in output):
			output[i]=["",0,0]
		for j in newdict[i]:
			if(newdict[i][j][1]>=10 and output[i][2]<newdict[i][j][2]):
				output[i][0]=j
				output[i][1]=newdict[i][j][0]
				output[i][2]=newdict[i][j][2]
			elif(newdict[i][j][1]>=10 and output[i][2]==newdict[i][j][2] and output[i][1] < newdict[i][j][1]): 
				output[i][0]=j
				output[i][1]=newdict[i][j][0]
				output[i][2]=newdict[i][j][2]
def disp(output):
	for i in sorted(output.keys()):
		print(i+","+output[i][0])
strike(output)
disp(output)
	
	























	
