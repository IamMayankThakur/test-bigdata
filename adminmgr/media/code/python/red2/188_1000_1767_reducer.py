#!/usr/bin/python
import csv
from operator import itemgetter
import sys

global no_balls
global no_runs
dictionary = dict()
new_dictionary = dict()
for line in sys.stdin:
    line = line.strip()
    line_val = line.split("\t")
    key, val = line_val[0], line_val[1]
    try:
        count = int(val)
    except ValueError:
        continue
    
    if (key not in dictionary):
        dictionary[key] = (int (val),1)
    else:
        dictionary[key] = (dictionary[key][0]+ int(val),dictionary[key][1]+1)
        
for key in dictionary:
    if(dictionary[key][1] > 5):
        new_dictionary[key] = (dictionary[key][0],dictionary[key][1])

dictionary_2 = sorted(new_dictionary.items())

dictionary_1 = sorted(dictionary_2, key = lambda x:x[1])

dictionary = sorted(dictionary_1, key=lambda x:x[1][0],reverse=True)

for i in range(len(dictionary)):
     print('%s,%s,%s' % (dictionary[i][0],dictionary[i][1][0],dictionary[i][1][1]))
