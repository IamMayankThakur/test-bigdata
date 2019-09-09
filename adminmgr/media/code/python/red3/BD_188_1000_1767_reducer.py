#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

dictionary = dict()
new_dictionary = list()
count = 0
for line in sys.stdin:
    line = line.strip()
    line_val = line.split("\t")
    key, val = line_val[0], line_val[1]
    try:
        count = int(val)
    except ValueError:
        continue
    
    if (key not in dictionary):
        dictionary[key] = (int(val),1)
    else:
        dictionary[key] = (dictionary[key][0]+ int(val),dictionary[key][1]+1)

# print(dictionary)
        
for key in dictionary:
    line_val = key.split(";")
    if(dictionary[key][1] >= 10):
        new_dictionary.append([line_val[0],line_val[1],(dictionary[key][0]*100/dictionary[key][1]),dictionary[key][0]])

# print(new_dictionary)
dictionary_3 = sorted(new_dictionary, key= lambda x:x[3],reverse=True) 
dictionary_2 = sorted(dictionary_3, key= lambda x:x[2],reverse=True) 
dictionary_1 = sorted(dictionary_2, key = lambda x:x[0])

var = dictionary_1[0][0]
print('%s,%s' % (dictionary_1[0][0],dictionary_1[0][1]))
for i in range(len(dictionary_1)):
    curr_var = dictionary_1[i][0]
    if (curr_var != var ):
        print('%s,%s' % (dictionary_1[i][0],dictionary_1[i][1]))
    var = curr_var                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    