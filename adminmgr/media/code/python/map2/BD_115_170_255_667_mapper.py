#!/usr/bin/python3
import sys
import csv

i = 0
infile = sys.stdin
for line in infile:
    line = line.strip()
    values = line.split(',')
    i = i + 1
    
    if(len(values) < 7):
        continue
    '''if(values[7] >= 0 and values[8] > 0): #for both
        print('%s,%s,%d,%d' % (values[6], values[4], values[8] + values[7], 0))
    if(values[8] > 0 and values[7] == 0): #for extras, ball not counted but runs counted 
        print('%s,%s,%d,%d' % (values[6], values[4], values[8], 1))
    if(values[7] >= 0 and values[8] == 0): #for runs
         print('%s,%s,%d,%d' % (values[6], values[4], values[7], 1)) '''
    runs = int(values[7])
    extras = int(values[8])
    if(runs != 0 and extras != 0): #eg 1 1
        print('%s,%s,%d,%d' % (values[6], values[4], runs + extras, 1))
    elif(extras != 0 and runs == 0): #eg 0 1
        print('%s,%s,%d,%d' % (values[6], values[4], extras, 1))
    elif(runs != 0 and extras == 0): #eg 4 0
        print('%s,%s,%d,%d' % (values[6], values[4], runs, 1))
    else: #eg 0 0
        print('%s,%s,%d,%d' % (values[6], values[4], 0, 1))
        
