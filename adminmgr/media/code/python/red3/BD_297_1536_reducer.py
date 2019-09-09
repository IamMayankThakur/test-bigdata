#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)  #used to correct a KeyError

global current_key
current_key = ""
deliveries = {}
runs = {}
final_dict = {}

for line in sys.stdin:   #line by line input from the mapper
    line = line.strip()
    line_val = line.split("\t")  #split line based on tab
    venue_batsman, val = line_val[0], line_val[1]
    run_per_batsman = int(line_val[2])
    if current_key == venue_batsman:   #if the current key is same as the previous key
        if current_key in deliveries:   #increasing count of the deliveries faced by a batsman in a particular venue
                deliveries[current_key] += 1
        else:
                deliveries[current_key] = 1
       
        if current_key in runs:         #adding the runs the batsman has hit in that venue
              runs[current_key] += run_per_batsman
        else:
              runs[current_key] = run_per_batsman


    else:                            #if the previous key is different from the current key (happens when it is not ordered)

        current_key = venue_batsman
        if current_key in deliveries:
                deliveries[current_key] += 1
        else:
                deliveries[current_key] = 1
       
        if current_key in runs:
                runs[current_key] += run_per_batsman
        else:
                runs[current_key] = run_per_batsman
	
for k,v in deliveries.items():   #used to calculate strike rate for each batsman at each venue and appending batsman,strike rate as a list to value of 																	final dictionary
	if(v>=10 and (k in runs)):   #number of deliveries faced should be >=10 
		strike_rate=(runs[k]*100)/deliveries[k]   
		ven,bat=k.split('::')   #splitting the key passed from mapper into venue and batsman separately
		if ven in final_dict:
			final_dict[ven].append([bat,strike_rate])
		else:
			final_dict[ven]=[[bat,strike_rate]]

final_list=[]
for k2,v2 in final_dict.items():   #for finding the maximum strike rate
	max_rate=0
	for i in v2:          
		if(i[1]>max_rate):   #if the new value is more that present max rate, change the maximum
			max_rate=i[1]
			max_batsman=i[0]
		elif(i[1]==max_rate):  #if the new value is same as present max rate, take the one with greater runs
			present=k2+'::'+i[0]
			previous=k2+'::'+max_batsman
			if(int(runs[present])>int(runs[previous])):  #checking runs after combining the venue and batsman for comparision in runs dict
				max_rate=i[1]
				max_batsman=i[0]
	final_list.append([k2,max_batsman])

sorted_final = sorted(final_list)	 #sorts based on alphabetical order
for j in sorted_final:
	print ('%s,%s' % (j[0],j[1]))
				

