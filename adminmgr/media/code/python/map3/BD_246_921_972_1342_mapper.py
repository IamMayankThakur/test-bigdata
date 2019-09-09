#!/usr/bin/python3
import sys
import csv

dict_ven = {}
list_ven = []
infile = sys.stdin

venue = ''  # initially empty string
#csv_input=r'C:\Users\USER i5\Downloads\alldata.csv'
for line in infile:
    line = line.strip()
    row_list = line.split(',')
    # key,value : (Venue,Batsman),[no of deliveries,no of runs]
    # exclude cases with extras
    # storing venue for each match
    # print(row_list)
    if(row_list[0] == 'info' and row_list[1] == 'venue'):
        if(len(row_list) == 4):
            row_list[2] = row_list[2]+','+row_list[3]
            venue = row_list[2][1:-1]
        else:
            venue = row_list[2]
        if(venue not in list_ven):
            list_ven.append(venue)
        # recording values for each match, taking in only values which are not extras
    elif(row_list[0] == 'ball' and int(row_list[8]) == 0):
        key = venue+'~'+row_list[4]
        if(venue not in dict_ven):
            dict_ven.update({venue: 1})
        else:
            dict_ven[venue] = dict_ven[venue]+1
        val = [1]  # no_of_deliveries for 1 record
        val.append(int(row_list[7]))  # no_of_runs in record
        print('%s:%s' % (key, str(val)))
    else:
        continue
