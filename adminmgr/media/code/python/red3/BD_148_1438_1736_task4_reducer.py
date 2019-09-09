#!/usr/bin/python3

import csv
import sys
infile=sys.stdin
overall = {}
final=[]
for line in infile:
   line = line.strip()
   info = line.split("\t")
   venue = info[0]
   batrun = info[1].split(",")
   batsman = batrun[0]
   runs = int(batrun[1])
   if venue not in overall:
      overall[venue]=dict()
   if batsman in overall[venue]:
      overall[venue][batsman][1] += 1 #number of balls
      overall[venue][batsman][0] += runs #number of runs
   elif batsman not in overall[venue]:
      overall[venue][batsman]=[0,1]
      overall[venue][batsman][0] = runs 
for venue in list(overall):
   for batsman in list(overall[venue]):
      if overall[venue][batsman][1] < 10:
         del overall[venue][batsman]


for venue in overall:
   for batsman in overall[venue]:
      sr= (overall[venue][batsman][0]*100)/overall[venue][batsman][1]
      overall[venue][batsman].append(sr)

for venue in overall:
	maxsr=list(overall[venue].keys())[0]
	for batsman in overall[venue]:
		if(overall[venue][batsman][2] > overall[venue][maxsr][2] or (overall[venue][batsman][2] == overall[venue][maxsr][2] and overall[venue][batsman][0] > overall[venue][maxsr][0])):
			maxsr=batsman
	final.append((venue,maxsr))	
final.sort()	
for i in final:
	print(i[0]+','+i[1])
