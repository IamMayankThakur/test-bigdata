#!/usr/bin/python3
#Reducer.py
from operator import itemgetter
import sys
dict1 = {}
for line in sys.stdin:
    line = line.strip()
    words= line.split('\t')
    a = (words[0],words[1])
    
    if a not in dict1.keys():
        dict1[a] =(str(int(words[2])+int(words[4])),1)
    else:
        rn = int(dict1[a][0])
        cn = int(dict1[a][1])
        dict1[a]  = (str(rn+int(words[2])+int(words[4])),str(cn+1))
dict2 = sorted(dict1.items(), key= lambda x: int(x[1][0]),reverse=True)

for m in range(500):
    for l in range(len(dict2)-1):
        tup1= dict2[l]
        tup2= dict2[l+1]
        if int(tup1[1][0]) == int(tup2[1][0]):
            if int(tup1[1][1]) == int(tup2[1][1]):
                if  tup1[0][1] == tup2[0][1]:
                    if tup1[0][0] > tup2[0][0]:
                        temp = dict2[l]
                        dict2[l] = dict2[l+1]
                        dict2[l+1] = temp
                if tup1[0][1] > tup2[0][1]:
                    temp = dict2[l]
                    dict2[l] = dict2[l+1]
                    dict2[l+1] = temp
            if int(tup1[1][1]) > int(tup2[1][1]):
                temp = dict2[l]
                dict2[l] = dict2[l+1]
                dict2[l+1] = temp
                

for u in dict2:
    if int(u[1][1])>5:
        print(u[0][1].strip()+","+u[0][0].strip()+","+u[1][0].strip()+","+u[1][1].strip()+'\t')



#for f in dict1.keys():
#    if int(dict1[f][1]) >5:
#      	print(f[0],",",f[1],",",dict1[f][0],",",dict1[f][1])
