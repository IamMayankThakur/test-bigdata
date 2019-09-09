#!/usr/bin/python3

import sys

key = ''
ls = []

for record in sys.stdin:
    key_new, val = record.split("\t");
    val = [int(k) for k in val.split(",")]
    if(key_new != key):
        ls.append([key_new, val[0], val[1]])
        key = key_new
    else:
        ls[len(ls)-1][1]+=val[0]
        ls[len(ls)-1][2]+=val[1]

ls_temp = []
#filtering the records having more than 5 deliveries
for x in ls:
    if(x[2] >= 6):
        ls_temp.append(tuple(x))


#sorting based on the #of wickets in descending order, global sorting
sorted_list = list(sorted(ls_temp, key = lambda x: x[1], reverse = True))


#sorting the records based on the no. of deliveries  having same no. of wickets, local sorting
start_index = 0
end_index = 0
for i in range(1, len(sorted_list)):
    if(sorted_list[i][1] == sorted_list[i-1][1]):
        end_index+=1
    elif((end_index-start_index)>0):
        sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:x[2])
        start_index = i
        end_index = i
    else:
        start_index = i
        end_index = i

#taking care after finishing the list traversal
if((end_index - start_index) >0):
    sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:x[2])
    start_index = i
    end_index = i


for item in sorted_list:
	a=int(item[1])
	b=int(item[2])
	print('%s,%d,%d'%(item[0],a,b))
