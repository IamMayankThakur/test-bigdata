#!/usr/bin/env python

import sys

dict = {}
for line in sys.stdin:
#the dictionary has 'key':value pairs such that key is 'venue,batsman' and value is [runs, ball]
	line=line.strip()
	ve,ba,a,b= line.split(',')
	f=ve+','+ba	
   # print(a,b)
	if(dict.get(f) == None):	
        #a, b = z[1].split(',')
        	dict[f] = [int(a), int(b)]
	#print("here\n")
	else:
		dict[f][0]+=int(a)
		dict[f][1]+=int(b)
	
#print(dict)
#filtering out the key-value pairs having more than 10 deliveries

for key in dict.keys():
    if(dict[key][1]<10):
        del dict[key]
#print(dict)
#adding the strike-rate
for key in dict.keys():
    dict[key].append((dict[key][0]*100)//dict[key][1])


list_a = [[x[0].split(',')[0], x[0].split(',')[1], x[1][0], x[1][1], x[1][2] ]for x in dict.items()]
#each element is a list of 5 subelements[venue, batsman, runs, balls, strike-rate]

#sorting based on venue(global sort)
sorted_list = sorted(list_a, key = lambda x: x[0])


#sorting based on strike-rate for the same venue(local sorting)
start_index= 0
end_index = 0
for i in range(1, len(sorted_list)):
    if(sorted_list[i][0] == sorted_list[i-1][0]):
        end_index+=1
    elif((end_index-start_index)>0):
        sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:x[4], reverse = True)

        #selecting the record with highest strike rate and runs
        ele = sorted_list[start_index]
        for j in sorted_list[start_index+1:end_index+1]:
            if(ele[4] == j[4] and ele[2] < j[2]):
                ele = j
        print("%s,%s"%(ele[0],ele[1]))

        start_index = i
        end_index = i
    else:
        print(sorted_list[start_index][0] + ',' + sorted_list[start_index][1] )
        start_index = i
        end_index = i

#taking care after finishing the list traversal
if((end_index - start_index) >0):
    sorted_list[start_index:end_index+1] = sorted(sorted_list[start_index:end_index+1], key = lambda x:x[4], reverse = True)

    #selecting the record with highest strike rate and runs
    ele = sorted_list[start_index]
    for j in sorted_list[start_index+1:end_index+1]:
        if(ele[4] == j[4] and ele[2] < j[2]):
		ele = j
    y=str(ele[0])
    z=str(ele[1])
    print('%s,%s'%(y,z))
