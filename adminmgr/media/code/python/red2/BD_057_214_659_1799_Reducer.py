#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import itertools
dictionary=dict()
i=0
for val in sys.stdin:                                                   #to extract every line one at a time
    val=val.strip()                                                     #removes whitespaces
    new_val=val.split(",")                                              #each line is split into values
    key1=new_val[0]
    key2=new_val[1]
    key3=int(new_val[2])
    key4=int(new_val[3])
    if((key1,key2) in dictionary.keys()):
        d=dictionary[key1,key2]
        dictionary[key1,key2]=(key3+d[0] , key4+d[1]) 
    else:
        dictionary[key1,key2]=(key3,key4)
for k,v in list(dictionary.items()):
    v1,v2=v[0],v[1]
    if(v[1] < 6):
        del dictionary[k]
old=[]
new=sorted(dictionary.items(),key=lambda x:(-x[1][0] , x[1][1]))
for k in range(len(new)):
  print(new[k][0][0]+","+new[k][0][1]+","+str(new[k][1][0])+","+str(new[k][1][1]))