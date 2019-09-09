#!/usr/bin/python3
import sys

dismisalList = ['caught and bowled', 'caught', 'bowled', 'stumped', 'lbw', 'hit wicket', 'obstructing the field']
nonDismisalList = ['run out', 'retired hurt', '""']

for line in sys.stdin:
    line = line.strip()
    myList = line.split(',')

    if (myList[0] == 'ball') :
        if myList[9] in nonDismisalList:
            print('%s\t%s\t%s' % (myList[4], myList[6], 0))
        else:
            print('%s\t%s\t%s' % (myList[4], myList[6], 1))
