#!/usr/bin/python3
import sys

venues = dict()
finalop = list()

for line in sys.stdin:
    line = line.strip()
    venue, batsman, runs, extra = line.split("\t")
    runs = int(runs)

    if(venue not in venues.keys()):
        venues[venue] = dict()

    if(batsman not in venues[venue].keys()):
        if(extra == "0"):
            venues[venue][batsman] = [runs, 1]
        else:
            venues[venue][batsman] = [0, 0]
    else:
        if(extra == "0"):
            venues[venue][batsman][0] = venues[venue][batsman][0] + runs
            venues[venue][batsman][1] = venues[venue][batsman][1] + 1

for ven in venues.keys():
    maxsr = ["", 0, 0]
    for bman in venues[ven].keys():
        if(venues[ven][bman][1] != 0):
            sr = venues[ven][bman][0]/venues[ven][bman][1]
            if(venues[ven][bman][1] >= 10 and sr >= maxsr[2]):
                if((sr > maxsr[2]) or (sr == maxsr[2] and venues[ven][bman][0] > maxsr[1])):
                    maxsr = [bman, venues[ven][bman][0], sr]
    finalop.append([ven, maxsr[0]])

finalop.sort(key=lambda a: a[0])

for i in finalop:
    print("%s,%s" % (i[0], i[1]))
