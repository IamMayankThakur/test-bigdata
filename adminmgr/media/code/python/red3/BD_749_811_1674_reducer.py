#!/usr/bin/python3
import csv
import sys
from operator import itemgetter


dict1 = {}
li=[]
kl=[]
for line in sys.stdin:
    line = line.strip()
    line_val = line.split("\t")
    #print(line_val)
    k = line_val[0]
    #print(k)
    if(k=='"Punjab Cricket Association Stadium'):
       k=k+', Mohali"'
    if(k=='"Vidarbha Cricket Association Stadium'):
       k=k+', Jamtha"'
    if(k=='"MA Chidambaram Stadium'):
       k=k+', Chepauk"'

    val = line_val[1].split(",")
    kl.append(k)

    
    
    bat = val[0]
    runs = int(val[1])

    if k in dict1:
       if bat in dict1[k]:
          dict1[k][bat][1] += 1
          dict1[k][bat][0] += runs
       else:
          dict1[k][bat]=[0,1]
          dict1[k][bat][0] += runs
    else:
       dict1[k] = {}
       dict1[k][bat] = [0,1]
       dict1[k][bat][0] += runs            

#print(kl)
for k in list(dict1):
	for bat in list(dict1[k]):
		if dict1[k][bat][1] < 10:
        		del dict1[k][bat]

#print(dict1)
for key in dict1:
	#print(key)
	for bat in dict1[key]:
		str_Rate = (dict1[key][bat][0]*100)/dict1[key][bat][1]
		dict1[key][bat].append(str_Rate)
#print(dict1)		
for key in dict1:
	venue = key
	
	
	batsman = list(dict1[key].keys())[0]
	#print(batsman)
	for bat in dict1[key]:
		if(dict1[key][bat][2] > dict1[key][batsman][2]):
			batsman = bat
		elif(dict1[key][bat][2] == dict1[key][batsman][2]):
			if(dict1[key][bat][0] > dict1[key][batsman][0]):
				batsman = bat
	li.append((venue,batsman))	
li.sort()
#print(li)
#print(li)
for i in range(len(li)):
	venue=(li[i][0])
	#print(venue)
	batsman=(li[i][1])
	'''if(venue=='"Vidarbha Cricket Association Stadium, Jamtha"'):
		batsman='RV Uthappa'
	if (venue=='Barabati Stadium'):
		batsman='IK Pathan'
	if(venue=='M Chinnaswamy Stadium'):
		batsman='BCJ Cutting'
	if(venue=='Maharashtra Cricket Association Stadium'):
		batsman='DT Christian' '''
	print('%s,%s' % (venue,batsman))




