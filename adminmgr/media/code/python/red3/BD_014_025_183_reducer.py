#!/usr/bin/python3
import sys
import csv
import operator

venue = ""

            
mapper = []
for line in sys.stdin:
	line = line.strip()
	venue, batsman, runs = line.split(';')
	    
	mapper.append(venue+";"+batsman+";"+runs)
            
                
#print(mapper)                


runs = dict()
balls = dict()
run_rate = dict()

stadiums = []

final = []

for line in mapper:
        l = line.split(";")
        #print(l)
        k = l[0] + ";" + l[1]
        if(k not in runs):
            runs[k] = int(l[2])
            balls[k] = 1
        else:
            runs[k] = runs[k] + int(l[2])
            balls[k] = balls[k] + 1

        if(l[0] not in stadiums):
            stadiums.append(l[0])
            
for y in balls:
    if(balls[y] >= 10):
        run_rate[y] = round((runs[y]*100)/balls[y], 2)
        
for x in stadiums:
    temp = []
    for z in run_rate:
        y = z.split(";")
        if(y[0] == x):
                temp.append(run_rate[z])
    if(len(temp) > 0):
        maximum = max(temp)
        players = []
        for z in run_rate:
            y = z.split(";")
            if(y[0] == x and run_rate[z] == maximum):
                players.append(z)
        if(len(players) == 1):
            final.append(players[0])
        else:
                m = 0
                player = ""
                for t in players:
                        if(runs[t] > m):
                                m = runs[t]
                                player = t         
                final.append(player)

        
final = sorted(final)
final_1 = []
for x in final:
        y = x.split(";")
        final_1.append(y[0]+","+y[1])
for x in final_1:
        print(x)
