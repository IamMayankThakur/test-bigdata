#!/usr/bin/python3
import sys

for line in sys.stdin:
    line = line.strip()
    line = line.split(",")
    try:
        print([line[6], line[4], int(line[7]) + int(line[8]), 1])
    except IndexError:
        pass