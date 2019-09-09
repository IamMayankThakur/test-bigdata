#!/usr/bin/python3

import sys

stadium = ''
score = 0

for records in sys.stdin.readlines():
    values = records.strip()
    if 'info,venue' in values:
        stadium = values[11:]
    elif 'ball' in values:
        values = values.split(',')
        if int(values[8]) == 0:
            print(stadium, '|', values[4], '|', values[7], '|', '1', sep='')
