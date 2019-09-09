#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import itertools


main_dict = dict()
i = 0

for line in sys.stdin:
    line = line.strip()
    line_val = line.split(",")
    key1,key2,key3,key4 = line_val[0], line_val[1],int(linearray[3])

    if(key1,key2) in main_dict.keys()):
       d = main_dict[key1,key2]
       main_dict[key1,key2] = (key3 + d[0] , key4+d[1])
    else:
       main_dict[key1,key2] = (key3,key4)



for k,v in list(main_dict.items()):
    v1,v2 = v[0],v[1]
    if(v[1]<0):
	del main_dict[k]
old = []
l=sorted(main_dict.items(),key = lambda x:(-x[1][0] , x[1][1]))

for k in range(len(l)):
  print(l[k][0][0]+","+l[k][0][1]+","+str(l[k][1][0])+","+l[k][1][1])
