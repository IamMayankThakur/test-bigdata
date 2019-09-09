import csv
from operator import itemgetter
import sys
import itertools

only_dict = dict()

for line in sys.stdin :
    line_val = (line.strip()).split(",")
    key1,key2,key3,key4 = line_val[0],line_val[1],int(line_val[2]),int(line_val[3])

    #appending bowler-batsman pair to dict along with values
    if((key1,key2) not in only_dict.keys()):
        only_dict[key1,key2] = (key3,key4)
    else:
        add_values = only_dict[key1,key2]
        only_dict[key1,key2] = (key3 + add_values[0], key4 + add_values[1]) 

l1 = list(only_dict.items())
for key,value in l1:
    if(value[1])>5):
        continue
    else:
        del only_dict[key] #deleting records having balls faced less than 5

l2 = sorted(only_dict.items(),key= lambda x:(-x[1][0], x[1][1]))

for i in range(len(l2)):
  print(l2[i][0][0]+","+l2[i][0][1]+","+str(l2[i][1][0])+","+str(l2[i][1][1]))