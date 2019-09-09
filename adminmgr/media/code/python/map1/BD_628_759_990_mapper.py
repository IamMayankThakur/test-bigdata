#!/usr/bin/python3
import sys

for line in sys.stdin:
    line = line.strip()
    line = line.split(",")
    try:
        if(line[9] == '""' or line[9] == "run out" or line[9] == "retired hurt"):
            print(str([line[4], line[6], 0, 1]))
        else:
            print(str([line[4], line[6], 1, 1]))
    except IndexError:
        continue
