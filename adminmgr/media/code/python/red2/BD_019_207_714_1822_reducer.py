#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import itertools

current_count = 0
current_key = ""
current_count = 0
current_key = ""
main_dict = dict()
i = 0

for line in sys.stdin: #each line
    line = line.strip() #removes whitespaces
    line_val = line.split(",") #splits each line into values
    key1,key2,key3,key4 = line_val[0], line_val[1],int(line_val[2]),int(line_val[3])

    if((key1,key2) in main_dict.keys()):
        d = main_dict[key1,key2]
        main_dict[key1,key2] = (key3 + d[0] , key4+d[1]) 
    else:
        main_dict[key1,key2] = (key3,key4)


for k,v in list(main_dict.items()):
    v1,v2 = v[0],v[1]
    #print(v1,v2)
    if(v[1] < 6):
        del main_dict[k]
old = []
l=sorted(main_dict.items(),key = lambda x:(-x[1][0] , x[1][1]))

for k in range(len(l)-1):
  print(l[k][0][0]+","+l[k][0][1]+","+str(l[k][1][0])+","+str(l[k][1][1]))

