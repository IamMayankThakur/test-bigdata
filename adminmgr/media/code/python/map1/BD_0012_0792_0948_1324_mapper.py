#!/usr/bin/python3
import sys

for ln in sys.stdin:
    ln = ln.strip()
    ln = ln.split(",")
    try:
        if(ln[9] != 'run out' and ln[9]!= '""' and ln[9]!='retired hurt'):
            print(str([ln[4], ln[6], 1, 1]))
        else:
            print(str([ln[4], ln[6], 0, 1]))
    except IndexError:
        pass
