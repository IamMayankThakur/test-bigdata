#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)

#fuel column index 8
for line in infile:
    line = line.strip()
    record = line.split(',')
    if(record[0] == 'ball'):
        if(record[10] == record[4] and record[9] != 'run out' and record[9] != 'retired hurt'):
            print("%s,%s,%s,%s" % (record[4], record[6], 1, 1))
        else:
            print("%s,%s,%s,%s" % (record[4], record[6], 0, 1))


