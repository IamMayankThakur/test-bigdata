#!/usr/bin/env python3
bat_bowl_list = []
bat_bowl_dict = {}
final_dict = {}
run_dict = {}
un_dict = {}
strike_dict = {}
import sys
from ast import literal_eval


def Sort(sub_li):
    l = len(sub_li)
    for i in range(0, l):
        for j in range(0, l - i - 1):
            if sub_li[j][2] < sub_li[j + 1][2]:
                tempo = sub_li[j]
                sub_li[j] = sub_li[j + 1]
                sub_li[j + 1] = tempo
            elif sub_li[j][2] == sub_li[j + 1][2]:
                if sub_li[j][3] > sub_li[j + 1][3]:
                    tempo = sub_li[j]
                    sub_li[j] = sub_li[j + 1]
                    sub_li[j + 1] = tempo
                elif sub_li[j][3] == sub_li[j + 1][3]:
                    if sub_li[j][0] > sub_li[j + 1][0]:
                        tempo = sub_li[j]
                        sub_li[j] = sub_li[j + 1]
                        sub_li[j + 1] = tempo
    return sub_li


for i in sys.stdin:
    l = literal_eval(i)
    bat_bowl_list.append(l)

for i in bat_bowl_list:
    key = (i[2], i[0])
    if key in bat_bowl_dict:
        bat_bowl_dict[key] = bat_bowl_dict[key] + 1
    else:
        bat_bowl_dict[key] = 1
    if int(i[1]) >= 0:
        if key in run_dict:
            run_dict[key] = run_dict[key] + int(i[1])
        else:
            run_dict[key] = int(i[1])


bat_keys = bat_bowl_dict.keys()
for i in bat_keys:
    if bat_bowl_dict[i] >= 10:
        final_dict[i] = bat_bowl_dict[i]

for i in final_dict.keys():
    key = i
    if key not in strike_dict:

        strike_dict[key] = (run_dict[key] * 100) / int(final_dict[key])
# print(strike_dict)

sort = sorted(strike_dict.keys())
max_dict = {}
# print(sort)
for i in sort:
    key = i[0]
    # print(key)
    if key in max_dict:
        # print(max_dict[key])
        if strike_dict[i] > maxv:
            max_dict[key] = (i[1], maxv, run_dict[i])
            maxv = strike_dict[i]
        elif strike_dict[i] == maxv:
            if run_dict[i] > max_dict[key][2]:
                max_dict[key] = (i[1], maxv, run_dict[i])
                maxv = strike_dict[i]

    else:
        maxv = strike_dict[i]
        # print('maxv:',maxv)
        max_dict[key] = (i[1], maxv, run_dict[i])
keys_sort = sorted(max_dict.keys())

for i in keys_sort:
    print ("%s,%s" % (i, max_dict[i][0]))
