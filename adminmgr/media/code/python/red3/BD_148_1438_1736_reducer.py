#!/usr/bin/python3
import csv
import sys
infile=sys.stdin
final=[]
overall = {}

def takeFirst(x):
	return x[0]
for line in infile:
	line = line.strip()
	info = line.split("\t")
	batrun = info[1].split(",")
	batsman = batrun[0]
	runs = int(batrun[1])
	venue = info[0]
	if venue not in overall:
		overall[venue]=dict() 
	if batsman in overall[venue]:
		overall[venue][batsman][0] += runs #number of runs
		overall[venue][batsman][1] += 1 #number of balls
	elif batsman not in overall[venue]:
		overall[venue][batsman]=[runs,1] #list to hold balls and runs


for venue in overall:
	for batsman in overall[venue]:
		if overall[venue][batsman][1] < 10:
			overall[venue][batsman][0]=-1
			overall[venue][batsman][1]=-1 #number of balls set as negative if balls faced is less than 10 to filter out

for venue in overall:
	for batsman in overall[venue]:
		if(overall[venue][batsman][0]>-1):
			sr= (overall[venue][batsman][0]*100)/overall[venue][batsman][1]
			overall[venue][batsman][1]=sr #overwriting number of balls as we no longer require it

for venue in overall:
	maxsr=list(overall[venue].keys())[0]
	for batsman in overall[venue]:
		if(overall[venue][batsman][1] > overall[venue][maxsr][1] or (overall[venue][batsman][1] == overall[venue][maxsr][1] and overall[venue][batsman][1] > overall[venue][maxsr][0])):
			maxsr=batsman
	final.append((venue,maxsr));
	
final.sort(key=takeFirst)	

for i in final:
	print(i[0]+','+i[1])         
	
	

