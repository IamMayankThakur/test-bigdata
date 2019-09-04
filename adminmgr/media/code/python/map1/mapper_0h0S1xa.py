#!/usr/bin/python
import sys
import csv
infile = sys.stdin
#next(infile)
#df = pd.read_csv("craigslistVehiclesFull.csv")

#city column index 1
#fuel column index 8
for line in infile:
    line = line.strip()
    my_list = line.split(';')
    key = my_list[1]
    fuel = my_list[8]
    value = "0"
    if(fuel == 'gas'):
        value = "1"
    print('%s\t%s' % (key,value))       
