#!/usr/bin/python3
from itertools import groupby
from operator import itemgetter
import sys
new_dict = {}
def sort_if_equal(res):
	i = 0
	for k in range(len(res)-1):
		if(res[k][1][1]==res[k+1][1][1]):
			pass
		elif(res[k][1][1]!=res[k+1][1][1]):
			res[i:k+1] = sorted(res[i:k+1],reverse = False,key = lambda n:(n[1][0],n[0][0]))
			i = k+1
	res[i:k+1] = sorted(res[i:k+1],reverse = False,key = lambda n:(n[1][0],n[0][0]))

def read_mapper():
	for row in sys.stdin:
		row = row.strip()
		bowl,bat,run,extras = row.split(',')
		k = (bowl,bat)
		if(k not in new_dict):
			new_dict[k] = [0,0]
		new_dict[k][1] += int(run)
		new_dict[k][1] += int(extras)
		new_dict[k][0] += 1
res = []
read_mapper()
def disp(res):
	for i in res:
		if(i[1][0]>5):
			print(",".join(i[0])+","+",".join([str(i[1][1]),str(i[1][0])]))

def sort_runs(res):
	for i in sorted(new_dict.items(),reverse=True,key=lambda n:n[1][1]):
		res.append(i)

sort_runs(res)
sort_if_equal(res)
disp(res)


