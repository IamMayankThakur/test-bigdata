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
        if (current_key != "" and current_ball_count > 5):
            value = (current_run_count,current_ball_count,current_key)            
            list_to_sort.append(value)
        current_run_count = run_count
        current_ball_count = 1
        current_key = key


if current_key == key and current_ball_count > 5:
    value = (current_run_count,current_ball_count,current_key)            
    list_to_sort.append(value)

list_to_sort.sort(key = lambda value:[-value[0],value[1],value[2]])
for i in list_to_sort:
    print('%s,%s,%s' % (i[2],str(i[0]),str(i[1])))
