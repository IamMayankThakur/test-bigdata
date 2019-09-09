#!/usr/bin/python3
import sys
wickets_count = {}
deliveries_count = {}

for line in sys.stdin:
    line = line.strip()
    try:
        if(deliveries_count == {}):
            deliveries_count = dict()
        if(wickets_count == {}):
            wickets_count = dict()
        names, delivery = line.split("--")
        deliveries_count[names] = deliveries_count.get(names, 0) + 1
        if(delivery != 'ball' and delivery != "run out" and delivery != "retired hurt"):
            wickets_count[names] = wickets_count.get(names, 0) + 1

    except ValueError:
        pass

for key in [key for key in deliveries_count.keys() if deliveries_count[key] <= 5]:
    del deliveries_count[key]
    if(key in wickets_count.keys()):
        del wickets_count[key]

updated_count = {}

for key in [key for key in deliveries_count.keys()]:
    if(key in wickets_count.keys()):
        updated_count[key] = [deliveries_count[key], wickets_count[key]]
    else:
        updated_count[key] = [deliveries_count[key], 0]

sorted_list = sorted(updated_count.items(),
                     key=lambda x: (-x[1][1], x[1][0], x[0].split("||")[0]))

for key, value in sorted_list:
    batsman, bowler = key.split('||')
    if(key in wickets_count.keys()):
        print("%s,%s,%d,%d" %
              (batsman, bowler, int(wickets_count[key]), int(value[0])))
    else:
        print("%s,%s,%d,%d" %
              (batsman, bowler, 0, int(value[0])))
