#!/usr/bin/python3
import sys
venue = ""
venue_list = []
for ln in sys.stdin:
    ln = ln.strip()
    if(ln.split(",", 2)[1] == "venue"):
        ln = ln.split(",", 2)
    else:
        ln = ln.split(",")
    try:
        if(int(ln[8]) != 0):
            continue
        print(str([venue, ln[4], int(ln[7]), 1]))
    except IndexError:
        if(ln[1] == "venue"):
            venue = ln[2]





































