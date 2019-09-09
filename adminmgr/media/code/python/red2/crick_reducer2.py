#!/usr/bin/env python

import sys

dict = {}
#each element of the dict has the structure like 'key':[runs+extras, deliveries] where key is 'Bowler,Batsman'

for record in sys.stdin:

    key, value = record.split(':')
    a,b = value.split(',')
    a = int(a)
    b = int(b)
    if(dict.get(key) == None):
        dict[key] = [a+b,1]
    else:
        dict[key][0] = dict[key][0]+a+b
        dict[key][1] += 1

#filtering records having more than 5 deliveries
for key in dict.keys():
    if(dict[key][1]<=5):
        del dict[key]

#sorting the records based on the runs in descneding order, global sorting
sorted_list = list(sorted(dict.items(), key = lambda x: x[1][0], reverse = True))


#sorting the records based on the no. of deliveries  having same no. of runs , local sorting
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



#sorting based on the alphabetical order of the bowler name for records having same wickets and same deliveries(local sorting)
start_index = 0
end_index = 0
for i in range(1, len(sorted_list)):
    if(sorted_list[i][1][0] == sorted_list[i-1][1][0] and sorted_list[i][1][1] == sorted_list[i-1][1][1]):
        end_index+=1
    elif((end_index-start_index)>0):
        sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:(x[0].split(','))[0])
      #  print(sorted_list[start_index])
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
	c=str(item[0])
        print('%s,%d,%d'%(c,a,b))
