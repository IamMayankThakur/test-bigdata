#!/usr/bin/python3
from operator import itemgetter
import sys
import csv

Dict = {}
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	bowler,batsman,delivery,runs = line_val[0],line_val[1],line_val[3],line_val[2]
	tup = (bowler,batsman)
	
	runs = int(runs)
	delivery = int(delivery)

	if(tup in Dict):
		Dict[tup][0]+=runs
		Dict[tup][1]+=delivery
	
	else:
		Dict[tup] = [runs,delivery]
		
lst_sort = []
for i in Dict:
	lst_sort.append([i[0], i[1],-1*Dict[i][0],Dict[i][1]])
lst = sorted(lst_sort, key=itemgetter(2,3,0,1))

for i in lst:
	if(i[3]>5):
		print('%s,%s,%s,%s'%(i[0],i[1],-1*i[2],i[3]))

'''
for t in lst:
	print('%s,%s,%d,%d'%(t[0][0],t[0][1],t[1][0],t[1][1]))
'''
	
	
	
	
	
