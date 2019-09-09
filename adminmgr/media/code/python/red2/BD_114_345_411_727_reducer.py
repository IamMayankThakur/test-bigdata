#!/usr/bin/python3


import sys

key = ''
ls = []
for record in sys.stdin:

    new_key, value = record.split('\t')
    a,b = value.split(',')
    a = int(a)
    b = int(b)
    if(key != new_key):
        ls.append([new_key, a+b, 1])
        key = new_key
    else:
        ls[len(ls)-1][1]+=(a+b)
        ls[len(ls)-1][2]+=1

#filtering records having more than 5 deliveries
ls_temp = []

for x in ls:
    if(x[2] >=6):
        ls_temp.append(tuple(x))#fortifying the data

#sorting the records based on the runs in descneding order, global sorting
sorted_list = list(sorted(ls_temp, key = lambda x: x[1], reverse = True))


#sorting the records based on the no. of deliveries  having same no. of runs , local sorting
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
	c=str(item[0])
	print('%s,%d,%d'%(c,a,b))
