#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

current_run_count = 0
current_ball_count = 0
current_key = ""
list_to_sort = []
for line in sys.stdin:
    line = line.strip()
    line_val = line.split("\t")
    key, runs, ball_val = line_val[0], line_val[1], line_val[2]
    try:
        run_count = int(runs)
        ball_count = int(ball_val)
    except ValueError:
        continue
    if current_key == key:
        current_run_count += run_count
        current_ball_count += ball_count
    else:
        if (current_key != "" and current_ball_count >= 10):
            venue,batsman = current_key.split("+")
            list_to_sort.append((venue,(current_run_count*100/current_ball_count),current_run_count,batsman))
        current_run_count = run_count
        current_ball_count = 1
        current_key = key

if current_key == key and current_ball_count > 10:
    venue,batsman = current_key.split("+")
    list_to_sort.append((venue,(current_run_count*100/current_ball_count),current_run_count,batsman))

list_to_sort.sort(key = lambda x:[x[0],-x[1],-x[2],x[3]])

current_venue = ""
print('%s,%s' % (list_to_sort[0][0],list_to_sort[0][3]))
for i in list_to_sort:
    venue = i[0]
    if current_venue == venue:
        pass
    else:
        if current_venue != "":
            print('%s,%s' % (i[0],i[3]))
        current_venue = venue