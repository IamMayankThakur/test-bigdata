#!/usr/bin/env python

import sys

dict = {}
#each dictionary element has the structure like : 'key':[wickets, deliveries] where key is 'Batsman,bowler'
for record in sys.stdin:
    key, val = record.split(":");
    val = [int(k) for k in val.split(",")]
    if(dict.get(key) == None):
        dict[key] = val
    else:
        dict[key][0]+=val[0]
        dict[key][1]+=val[1]

#filtering the records having more than 5 deliveries
for key in dict.keys():
    if(dict[key][1]<=5):
        del dict[key]

#sorting based on the #of wickets in descending order, global sorting
sorted_list = list(sorted(dict.items(), key = lambda x: x[1][0], reverse = True))
#dict.items() returns an iterable consisting of tuples like ('key', [wickets, deliveries])



#sorting the records based on the no. of deliveries  having same no. of wickets, local sorting
start_index = 0
end_index = 0
for i in range(1, len(sorted_list)):
    if(sorted_list[i][1][0] == sorted_list[i-1][1][0]):
        end_index+=1
    elif((end_index-start_index)>0):
        sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:x[1][1])
        start_index = i
        end_index = i
    else:
        start_index = i
        end_index = i

#taking care after finishing the list traversal
if((end_index - start_index) >0):
    sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:x[1][1])
    start_index = i
    end_index = i



#sorting based on the alphabetical order of the batsman name for records having same wickets and same deliveries(local sorting)
start_index = 0
end_index = 0
for i in range(1, len(sorted_list)):
    if(sorted_list[i][1][0] == sorted_list[i-1][1][0] and sorted_list[i][1][1] == sorted_list[i-1][1][1]):
        end_index+=1
    elif((end_index-start_index)>0):
        sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:(x[0].split(','))[0])
        start_index = i
        end_index = i
    else:
        start_index = i
        end_index = i

#taking care after finishing the list traversal
if((end_index - start_index) >0):
    sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:(x[0].split(','))[0])
    start_index = i
    end_index = i




for item in sorted_list:
	a=int(item[1][0])
	b=int(item[1][1])
        print('%s,%d,%d'%(item[0],a,b))
