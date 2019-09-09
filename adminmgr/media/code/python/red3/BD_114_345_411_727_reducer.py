#!/usr/bin/python3

import sys

ls = []
key = ''
for line in sys.stdin:

	line = line.strip()

	new_key, value = line.split('\t')

	runs = int(value.split(',')[0])
	ball = int(value.split(',')[1])

	if(key!=new_key):
		ls.append([new_key, runs, ball])
		key = new_key
	else:
		ls[len(ls)-1][1]+= runs
		ls[len(ls)-1][2]+= ball


#filtering out the key-value pairs having more than 10 deliveries and calculating their strike rate
sorted_list = []

for x in ls:
	if(x[2] >= 10):
		#splitting the key into venue and Batsman
		key_split = x[0].split(",")
		venue = key_split[0]
		if(venue[0] == '"'):
			venue = venue + ',' + key_split[1]
			strike_rate = float(x[1]*100/x[2])
			batsman = key_split[2]
			sorted_list.append((venue, batsman, strike_rate, x[1]))#fortifying the data
		else:
			batsman = key_split[1]
			strike_rate = float(x[1]*100/x[2])
			sorted_list.append((venue, batsman, strike_rate, x[1]))


#sorting based on strike-rate for the same venue(local sorting)
start_index= 0
end_index = 0
for i in range(1, len(sorted_list)):
    if(sorted_list[i][0] == sorted_list[i-1][0]):
        end_index+=1
    elif((end_index-start_index)>0):
        sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:x[2], reverse = True)

        #selecting the record with highest strike rate and runs
        ele = sorted_list[start_index]
        for j in sorted_list[start_index+1:end_index+1]:
            if(ele[2] == j[2] and ele[3] < j[3]):
                ele = j
        print("%s,%s"%(ele[0],ele[1]))

        start_index = i
        end_index = i
    else:
        print("%s,%s"%(sorted_list[start_index][0], sorted_list[start_index][1] ))
        start_index = i
        end_index = i

#taking care after finishing the list traversal
if((end_index - start_index) >0):
    sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:x[2], reverse = True)

    #selecting the record with highest strike rate and runs
    ele = sorted_list[start_index]
    for j in sorted_list[start_index+1:end_index+1]:
        if(ele[2] == j[2] and ele[3] < j[3]):
	        ele=j
    y=str(ele[0])
    z=str(ele[1])
    print('%s,%s'%(y,z))
