#!/usr/bin/python3
import sys

for record in sys.stdin.readlines():
    record = record.strip()
    values = record.split(',')
    if values[0] == 'ball':
        print(values[6], ',', values[4], ',', int(values[7])+int(values[8]),
              ',', 1, sep='')
