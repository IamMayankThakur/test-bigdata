#!/usr/bin/python
import sys
import csv
read_line = sys.stdin

#dict = {batsman1:[{bowler1:balls_faced,wickets},{bowler2:balls_faced,wickets}...]

records = dict()

for line in read_line:
    line = line.strip() #remove white spaces
    new_list = line.split(',')
    if new_list[5] not in records:
        records[new_list[5]] = list()
        if new_list[10] == '': #batsman not out
            records[new_list[5]].append({new_list[7]:1, "wickets": 0})
        elif new_list[10] == "run out":
            records[new_list[5]].append({new_list[7] : 1, "wickets" : 0})
        else :
            records[new_list[5]].append({new_list[7] : 1, "wickets" : 1})

    else: #if batsman record already there
        bowlers_list = records[new_list[5]]
        for bowlers in bowlers_list:
            if new_list[7] in bowlers:
                if new_list[10] == '':
                    bowlers[new_list[7]] = bowlers[new_list[7]] + 1
                elif new_list[10] == "run out":
                    bowlers[new_list[7]] = bowlers[new_list[7]] + 1
                else:
                    bowlers[new_list[7]] = bowlers[new_list[7]] + 1
                    bowlers['wickets'] = bowlers['wickets'] + 1

            else:
                if new_list[10] == '': #batsman not out
                    records[new_list[5]].append({new_list[7]:1, "wickets": 0})
                elif new_list[10] == "run out":
                    records[new_list[5]].append({new_list[7] : 1, "wickets" : 0})
                else :
                    records[new_list[5]].append({new_list[7] : 1, "wickets" : 1})
        
            
            
        
                    
        
