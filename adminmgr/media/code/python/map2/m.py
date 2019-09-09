#!/usr/bin/python3
import sys

for line in sys.stdin:
    record=line.strip();
    fields=record.split(",");
    if(fields[0]=="ball"):
        map_out=(fields[6],fields[4])+(int(fields[7])+int(fields[8]),1);
        print(map_out)
