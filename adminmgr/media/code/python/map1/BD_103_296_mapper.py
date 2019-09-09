#!/usr/bin/python3
import sys

for record in sys.stdin.readlines():
    record = record.strip()
    values = record.split(',')
    if values[0] == 'ball':
        if values[4] == values[10] and \
                    not(values[9] == 'run out' or values[9] == 'retired hurt'):
            print(values[4], ',', values[6], ',', 1, ',', 1, sep='')
        else:
            print(values[4], ',', values[6], ',', 0, ',', 1, sep='')
