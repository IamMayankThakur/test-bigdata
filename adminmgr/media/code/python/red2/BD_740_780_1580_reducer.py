#!/usr/bin/python3
import csv
from operator import itemgetter		#itemgetter is a key function used to used to get the first item from the list-like object
import sys
import operator
#global variables
global number_del
global runs
global list_batbowl_comb
#initial declarations

runs=0
list_batbowl_comb=['','']
result_dict={}
number_del=0


if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")
for line in sys.stdin:
	line=line.strip()
	line_val=line.split(",")
	key,val=(line_val[0],line_val[1]),[int(line_val[2]),int(line_val[3])]
	try:
		num_del=val[0]
		#take those values from key val pair
		num_runs=val[1]
	except ValueError:
		continue

	if((list_batbowl_comb[0]==key[0]) and (list_batbowl_comb[1]==key[1])): #increment no of runs and number_del for a partcular batsman,bowler pair
		number_del+=num_del
		runs+=num_runs
	else:
		if(list_batbowl_comb[0]!="" and list_batbowl_comb[1]!=""  and number_del>5): #the number of del >5

			result_dict[(list_batbowl_comb[0],list_batbowl_comb[1])]=[runs,number_del] #append dict values for respective batsmen,bowler pair as runs and delivries as their values 

		number_del=1
		runs=num_runs
		list_batbowl_comb[0]=key[0]
		list_batbowl_comb[1]=key[1]


if(list_batbowl_comb[0]!="" and list_batbowl_comb[1]!="" and number_del>5):
			#append to res dictionary
			result_dict[(list_batbowl_comb[0],list_batbowl_comb[1])]=[runs,number_del]
			





sorted_op=sorted(result_dict.items(),key =lambda x:(-x[1][0],x[1][1],x[0][1])) #based on bowler name	


#result_dict.items() returns a list of tuples 
	
for i in sorted_op:

	print('%s,%s,%s,%s' % (i[0][1],i[0][0],i[1][0],i[1][1]))