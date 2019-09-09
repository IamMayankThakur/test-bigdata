#!/usr/bin/env python3
import csv

bat_bowl_dict = {}
bat_bowl_list = []
final_dict = {}
caught_dict = {}

import sys

list1 = []
venue = ""
r = sys.stdin
for i in r:
    str2 = i.strip()
    str1 = str2.split(",")
    list1.append(str1)


for row in list1:
    if len(row) > 4 and row[8] == "0":
        bat_bowl = (row[4], row[7], venue, 1)
        bat_bowl_list.append(bat_bowl)
        print(bat_bowl)
    else:
        if row[1] == "venue":
            if len(row) > 3:
                venue = row[2] +','+ row[3]
            else:
                venue = row[2]
