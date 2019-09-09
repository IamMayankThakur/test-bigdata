#!/usr/bin/python3

import os
import sys

d = dict()
venue = ""
count = 0
for line in sys.stdin:
        try:
                line = line.strip()
                if line[0:4] == "info":
                        values = line.split(",",2)
                        if "venue" in values:
                                venue = values[2]
                                #print(count,venue)

                if line[0:4] == "ball":
                        values = line.split(",")
                        if (values[4],venue) not in d:
                                d[(values[4],venue)] = [0,0]

                        if int(values[8]) == 0:
                                d[(values[4],venue)][0] += int(values[7])
                                d[(values[4],venue)][1] += 1
                        if venue == "":
                                print(count)

                count+= 1

        except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                #print(message)

for i in d:
        print(i[1],i[0],d[i][0],d[i][1],sep = "|")