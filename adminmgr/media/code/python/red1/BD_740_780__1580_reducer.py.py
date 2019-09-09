#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import operator
#initializing variables
wickets=0
result_dict={}
num_del=0
#to accomodate bat_bowl combinations
list=['','']
for line in sys.stdin:
	line=line.strip()
	line_val=line.split("\t")
	keys,val=(line_val[0]),[int(line_val[1]),int(line_val[2])] #to classify into key value pairs
	key=keys.split(',')
	ip_del=val[1]
	ip_wickets=val[0]
	if list[0]==key[0] and list[1]==key[1]:
		num_del+=ip_del
		wickets+=ip_wickets
		
	else:
		if(list[0]!="" and list[1]!="" and num_del>5):

			result_dict[(list[1],list[0])]=[wickets,num_del]

		num_del=1
		wickets=ip_wickets
		
		list[0]=key[0]
		list[1]=key[1]

if(list[0]!="" and list[1]!="" and num_del>5):
			
			result_dict[(list[1],list[0])]=[wickets,num_del]

sorted_x=sorted(result_dict.items(),key =lambda x:(-x[1][0],x[1][1],x[0][0]))
#to get the sorted result accordingly
	
for i in sorted_x:
	
	print('%s,%s,%d,%d' % (i[0][0],i[0][1],i[1][0],i[1][1]))
	


