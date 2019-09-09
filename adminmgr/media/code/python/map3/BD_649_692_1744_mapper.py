#!/usr/bin/python
import sys
import csv

dict_venue = dict()
list_venue = list()

venue = ''
for l in sys.stdin:
    row = l.strip().split(',')
    if(row[0] == 'ball' and int(row[8]) == 0):
        key = venue + '~' + row[4]
        if(venue in dict_venue):
            dict_venue[venue] = dict_venue[venue] + 1
        else:
            dict_venue.update({venue: 1})
           
        value = []
        value.append(1)
        value.append(int(row[7]))  
        print('%s:%s' % (key, str(value)))
    elif(row[0] == 'info' and row[1] == 'venue'):
        if(len(row) == 4):
            row[2] = row[2] + row[3]
            venue = row[2][1:-1]
        else:
            venue = row[2]
        if(venue not in list_venue):
            list_venue.append(venue)

    else:
        continue
        

