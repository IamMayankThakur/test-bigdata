#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

dictionary = {}

for line in sys.stdin:
    line = line.strip().split(":")
    value = line[1]
    
    inter = value.split("]")
    value = inter[0]
    
    inter = value.split("[")
    value = inter[1]
    
    value = value.split(",")
    
    no_of_balls = int(value[0])
    no_of_wickets = int(value[1])
    
    key = line[0].split("'")
    key = (key[1], key[3])

   
    if(key not in dictionary.keys()):
        dictionary.update({key: [no_of_balls, no_of_wickets]})
        
    else:
        tot1 = dictionary[key][1] + no_of_wickets
        dictionary[key][1] = tot1
        tot2 = dictionary[key][0] + no_of_balls
        dictionary[key][0] = tot2


dictionary = sorted(dictionary.items(), key=lambda condition: (-condition[1][1],condition[1][0],condition[0][0],condition[0][1]), reverse=False)



for iteration in dictionary:
    if(iteration[1][0] >= 6):
        print('%s,%s,%d,%d' %  (iteration[0][0], iteration[0][1], iteration[1][1], iteration[1][0]))
        
       
