#!/usr/bin/python3
from operator import itemgetter
import sys


main_dict = dict()
i = 0

for line in sys.stdin: 
    line = line.strip() 
    fileread = line.split("$")
    k1=fileread[0]
    k2=fileread[1]
    k3=int(fileread[2])
    k4=int(fileread[3])
    if((k1,k2) in main_dict.keys()):
        d = main_dict[k1,k2]
        main_dict[k1,k2] = (k3 + d[0] , k4+d[1]) 
    else:
        main_dict[k1,k2] = (k3,k4)


for item1,v in list(main_dict.items()):
    v1,v2 = v[0],v[1]
    #print(v1,v2)
    if(v[1] < 6):
        del main_dict[item1]
old = []
sorted_final=sorted(main_dict.items(),key = lambda item:(-item[1][0] , item[1][1]))
for item1 in range(len(sorted_final)):
  print(sorted_final[item1][0][0]+","+sorted_final[item1][0][1]+","+str(sorted_final[item1][1][0])+","+str(sorted_final[item1][1][1]))

