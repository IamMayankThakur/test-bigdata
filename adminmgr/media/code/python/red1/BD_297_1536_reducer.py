#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

global current_key

current_key = ""
#contains the number of deliveries for each batsman,bowler pair
dic = {}
#contains the number of wickets taken for each batsman bowler pair
dic_out={}
# sorted dictionary
final_dict={}

for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	# key = batsman, bowler pair , val=1
	key, val = line_val[0], line_val[1]
	# string that contains information about the wicket taken
	val2=line_val[2]
	try:
		count = int(val)
	except ValueError:
		continue
	# comapring the previous and the present key
	if current_key == key:
		# if the key is already present in the dictionary
		if current_key in dic:
			dic[current_key] += 1
		#else create a new key
		else:
			dic[current_key] = 1
		# no. of wickets taken
		if(val2=='caught' or val2=='bowled' or val2=='stumped' or val2=='caught and bowled' or val2=='lbw' or val2=='hit wicket' or val2=='obstructing the field'):
			if current_key in dic_out:
				dic_out[current_key] += 1
			else:
				dic_out[current_key] = 1
		# add zero to the value if the wicket is not taken
		else:
			if current_key not in dic_out:
				dic_out[current_key]=0
	# when the previous and present key are not the same 
	else:
		current_key = key
		if current_key in dic:
			dic[current_key] += 1
		else:
			dic[current_key] = 1
		if(val2=='caught' or val2=='bowled' or val2=='stumped' or val2=='caught and bowled' or val2=='lbw' or val2=='hit wicket' or val2=='obstructing the field'):
			if current_key in dic_out:
				dic_out[current_key] += 1
			else:
				dic_out[current_key] = 1
		else:
			if current_key not in dic_out:
				dic_out[current_key]=0


i=0
#create a final dictionary with the batsman-bowler pair as the key and deliveries,wickets as the value
for k,v in dic.items():
	if(v>5 and (k in dic_out)):
		final_dict[k]=[dic_out[k],dic[k]]
		

# sort the dictionary based on keys
list1=sorted(final_dict.items())
# ascending order of deliveries
list2=sorted(list1,key=lambda kv:kv[1][1])
#descending order of wickets
list3=sorted(list2,key=lambda x:x[1][0],reverse=True)
for i in list3:
	bb=i[0].split(',')
	print ('%s,%s,%d,%d\t' % (bb[0],bb[1],i[1][0],i[1][1]))



