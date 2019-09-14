#!/usr/bin/python3
import sys
import csv

dict_venue = {}
list_ven = []
infile = sys.stdin

venue = ''
for l in infile:
   # l = l.strip()
   #row_list = l.split(',')
    row_list = l.strip().split(',')
    if(row_list[0] == 'ball' and int(row_list[8]) == 0):
        key = venue + '~' + row_list[4]
        if(venue in dict_venue):
            dict_venue[venue] = dict_venue[venue] + 1
        else:
            dict_venue.update({venue: 1})
           
        value = []
        value.append(1)
        value.append(int(row_list[7]))  
        print('%s:%s' % (key, str(value)))
    elif(row_list[0] == 'info' and row_list[1] == 'venue'):
        if(len(row_list) == 4):
            row_list[2] = row_list[2] + ',' + row_list[3]
            venue = row_list[2]
        else:
            venue = row_list[2]
        if(venue not in list_ven):
            list_ven.append(venue)

    else:
        continue
