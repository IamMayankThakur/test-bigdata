#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
# next(infile)

for line in infile:
    line = line.strip()
    my_list = line.split(',')

    # avoid lines starting with version and info to process
    if(my_list[0] == 'ball'):
        # val= [no_of_deliveries, no_of_runs, no_of_wickets]
        val = [1]  # no_of_deliveries for 1 record
        # batsman in column 4 and bowler in column 6
        key = (my_list[4], my_list[6])

        # ensuring that runouts and no wickets are not counted
        if(my_list[9] != '""' and my_list[9] != "run out" and my_list[9]!="retired hurt"):
            val.append(1)
        else:
            val.append(0)
        print('%s:%s'   %   (str(key), str(val)))
