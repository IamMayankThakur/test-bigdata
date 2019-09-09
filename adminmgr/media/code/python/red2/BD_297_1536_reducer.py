#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 
global current_key

current_key = ""
#contains the number of deliveries for each bowler,batsman pair
dic = {}
#contains the number of runs scored for each bowler,batsman pair
dic_out={}
# sorted dictionary
final_dict={}

for line in sys.stdin:
    line = line.strip()
    line_val = line.split("\t")
    #key = bowler,batsman pair , val = 1
    key, val = line_val[0], line_val[1]
    #runs scored
    val2=int(line_val[2])
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
        # append the runs scored by the batsman into the dictionary if the key is present
        if current_key in dic_out:
              dic_out[current_key] += val2
        # add the new key, value pair to the dictionary
        else:
              dic_out[current_key] = val2

    # when the previous and present key are not the same 
    else:
        current_key = key
        if current_key in dic:
                dic[current_key] += 1
        else:
                dic[current_key] = 1
       
        if current_key in dic_out:
                dic_out[current_key] += val2
        else:
                dic_out[current_key] = val2
    

i=0
#create a final dictionary with the batsman-bowler pair as the key and deliveries,wickets as the value
for k,v in dic.items():
        if(v>5 and (k in dic_out)):
                final_dict[k]=[dic_out[k],dic[k]]
#sort the dictinary based on keys
list1=sorted(final_dict.items())
# ascending order of deliveries
list2=sorted(list1,key=lambda kv:kv[1][1])
# descending order of runs scored
list3=sorted(list2,key=lambda x:x[1][0],reverse=True)
for i in list3:
       bb=i[0].split(',')
       print ('%s,%s,%d,%d' % (str(bb[0]),str(bb[1]),i[1][0],i[1][1]))



