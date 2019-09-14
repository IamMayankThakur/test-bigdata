#!/usr/bin/python3
from operator import itemgetter
import sys
import csv
import ast

Dict={}
for line in sys.stdin:
	lst = ast.literal_eval(line)
	tupple = (lst[0],lst[1])
	
	if(tupple not in Dict):
		Dict[tupple] = [lst[2],lst[3]]
		
	else:
		Dict[tupple][0]+=lst[2]
		Dict[tupple][1]+=lst[3]
'''
for i in Dict:
	if(Dict[i][0]>5):
	
		print('%s,%s,%s,%s'%(i[0],i[1],Dict[i][0],Dict[i][1]))
'''
lst_sort = []
for i in Dict:
	lst_sort.append([i[0], i[1],-1*Dict[i][0],Dict[i][1]])
lst = sorted(lst_sort, key=itemgetter(2,3,0,1))
j=0
for i in lst:
	if(i[3]>5):
		print('%s,%s,%s,%s'%(i[0],i[1],-1*i[2],i[3]))
	
	
	



