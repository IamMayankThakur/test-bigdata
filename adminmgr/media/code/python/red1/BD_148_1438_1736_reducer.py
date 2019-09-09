#!/usr/bin/python3
import csv
import sys

overall=dict()
infile=sys.stdin
final=list()
def maxi(**dict1):
	m=list(dict1.keys())[0]
	m=m[0]
	for bb in dict1:
		if(dict1[bb][0] > dict1[m][0] or (dict1[bb][0]==dict1[m][0] and dict1[bb][1]<dict1[m][1])):
			m=bb
		elif( dict1[bb][0]==dict1[m][0] and dict1[bb][1]==dict1[m][1]):
			m=min(bb,m)
	return m
for line in infile:
	line=line.strip()
	info=line.split("\t")
	balls=int(info[2])
	wicket=int(info[1])
	bb=info[0]
	if(bb not in overall):
		overall[bb]=[wicket,balls]
	else:
		overall[bb][0]+=wicket
		overall[bb][1]+=balls
a=list(overall.keys())
for bb in a:
	if(overall[bb][1]<=5):
		del overall[bb]

while(len(overall)>0):
	m=list(overall.keys())
	m=m[0]
	for bb in overall:
		if(overall[bb][0] > overall[m][0] or (overall[bb][0]==overall[m][0] and overall[bb][1]<overall[m][1])):
			m=bb
		elif( overall[bb][0]==overall[m][0] and overall[bb][1]==overall[m][1]):
			m=min(bb,m)
	print(m,overall[m][0],overall[m][1],sep=",")
	del overall[m]
