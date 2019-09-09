#!/usr/bin/python3

import sys
import os

result = dict()
batsman = ""
runs = 0
balls = 0
venue = ""



for line in sys.stdin:
    line = line.strip()
    values = line.split("|",4)
    
    if len(values) > 3:

        venue = values[0]

        if batsman == values[1]:
            runs += int(values[2])
            balls += int(values[3])

        else:
            #print(venue,values)
            if values[0] not in result and len(values) > 3:
                result[venue] = ["",0,0,0]
            
            try:
                strike_rate = round((runs/balls)*100,3)
                if (strike_rate > result[venue][1] or (strike_rate == result[venue][1] and runs > [venue][2])) and balls >= 10:
                    result[values[0]] = [batsman,strike_rate,runs,balls]
        
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                #print(message)


            batsman = values[1]
            runs = int(values[2])
            balls = int(values[3])
    

for i in result:
    print(i,result[i][0],sep=",")