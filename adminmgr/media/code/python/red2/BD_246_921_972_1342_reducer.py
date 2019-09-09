#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

# resultant dictionary
res = dict()
for line in sys.stdin:
    line = line.strip()
    line_val = line.split(":")
    key, val = line_val[0], line_val[1]
    # print(type(key),type(val))
    try:
        # formatting key
        key = key.split("'")
        key = (key[1], key[3])

        # formatting val
        val = val.split("[")
        val = val[1]
        val = val.split("]")
        val = val[0]
        val = val.split(",")

        ball_count = int(val[0])
        run_count = int(val[1])

    except ValueError:
        print("ValueError")
        continue

    if(key in res.keys()):
        res[key][0] += ball_count
        res[key][1] += run_count
    else:
        res.update({key: [ball_count, run_count]})

# -x[1][1] for descending order of runs
#  x[1][0] for ascending order of no of deliveries
#  x[0][0] for name of bowler
res = sorted(
    res.items(), key=lambda x: (-x[1][1], x[1][0], x[0][0], x[0][1]), reverse=False)

# output format: bowler,batsman,no_of_runs,no_of_deliveries
for rec in res:
    if(rec[1][0] > 5):
        print('%s,%s,%d,%d' % (rec[0][0], rec[0][1], rec[1][1], rec[1][0]))
