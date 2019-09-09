#!/usr/bin/env python3
import csv

bat_bowl_dict = {}
bat_bowl_list = []
final_dict = {}
caught_dict = {}
import sys

list1 = []
r = sys.stdin
for i in r:
    str2 = i.strip()
    str1 = str2.split(",")
    list1.append(str1)

for row in list1:
    if len(row) > 4:
        bat_bowl = (row[4], row[6], 1, row[9])
        bat_bowl_list.append(bat_bowl)
        print(bat_bowl)
