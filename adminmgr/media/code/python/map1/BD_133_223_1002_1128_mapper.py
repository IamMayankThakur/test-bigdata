#!/usr/bin/python3

import sys

for line in sys.stdin:
    record=line.strip()
    fields=record.split(",")
    if(fields[0]=="ball"):
        if(fields[9]=="caught" or fields[9]=="bowled" or fields[9]=="lbw" or fields[9]=="caught and bowled" or fields[9]=="stumped" or fields[9]=="hit wicket" or fields[9]=="obstructing the field"):
            map_out=(fields[4],fields[6])+(1,1)
        else:
            map_out=(fields[4],fields[6])+(0,1)
        print(map_out)
    

