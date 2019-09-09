#!/usr/bin/python3
from itertools import groupby 
from operator import itemgetter
import sys

new_dict = {}
def read_mapper():
	for row in sys.stdin:
		row = row.strip()
		batsman,bowler,run = row.split(',')
		key = (batsman,bowler)
		if(key not in new_dict):
			new_dict[key] = [0,0]
		if(run=='-5'):
			new_dict[key][1] += 1
		new_dict[key][0] += 1
read_mapper()
output = []
def sort(output):
	for i in sorted(new_dict.items(),reverse = True,key = lambda n:n[1][1]):
		output.append(i)
j = 0
def sort_if_equal(output):
	i = 0
	for j in range(len(output)-1):
		if(output[j][1][1]==output[j+1][1][1]):
			pass
		elif(output[j][1][1]!=output[j+1][1][1]):
			output[i:j+1] = sorted(output[i:j+1],reverse = False,key = lambda n:(n[1][0],n[0][0]))
			i = j+1
	output[i:j+1] = sorted(output[i:j+1],reverse = False,key = lambda n:(n[1][0],n[0][0]))
def disp(output):
	for i in output:
		if(i[1][0]>5):
			print(",".join(i[0])+","+",".join([str(i[1][1]),str(i[1][0])]))
sort(output)
sort_if_equal(output)
disp(output)
