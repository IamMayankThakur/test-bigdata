#!/usr/bin/python3
import sys

for line in sys.stdin:
    line = line.strip()
    myList = line.split(',')

    if (myList[0] == 'ball') :
        print('%s\t%s\t%s' % (myList[4], myList[6], myList[9]))
