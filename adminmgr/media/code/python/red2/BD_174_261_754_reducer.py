#!/usr/bin/python3
import sys
count_dict = {}

for line in sys.stdin:
    line = line.strip()
    data = list(map(str, line.split(",")))
    batsman, bowler, runs, extras = data[0], data[1], data[2], data[3]
    total_runs = int(runs)+int(extras)
    players_pair = (batsman, bowler)
    if players_pair not in count_dict:
        count_dict[players_pair] = []
        count_dict[players_pair].append(total_runs)
        count_dict[players_pair].append(1)

    else:
        count_dict[players_pair][0] = count_dict[players_pair][0]+total_runs
        count_dict[players_pair][1] = count_dict[players_pair][1]+1

sorted_list = []
sorted_list = sorted(
    count_dict, key=lambda x:  (-count_dict[x][0], count_dict[x][1], x[1]))

for key in sorted_list:
    if(count_dict[key][1] > 5):
        print("%s,%s,%d,%d" %
              (key[1], key[0], count_dict[key][0], count_dict[key][1]))
