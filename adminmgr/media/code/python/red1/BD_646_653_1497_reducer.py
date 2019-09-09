#!/usr/bin/env python3
bat_bowl_list = []
bat_bowl_dict = {}
final_dict = {}
caught_dict = {}
un_dict = {}
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
                    elif sub_li[j][0] == sub_li[j + 1][0]:
                        if sub_li[j][1] > sub_li[j + 1][1]:
                            tempo = sub_li[j]
                            sub_li[j] = sub_li[j + 1]
                            sub_li[j + 1] = tempo
    return sub_li


def cmp(a, b):
    return (a > b) - (a < b)


for i in sys.stdin:
    l = literal_eval(i)
    bat_bowl_list.append(l)

for i in bat_bowl_list:
    key = (i[0], i[1])
    if key in bat_bowl_dict:
        bat_bowl_dict[key] = bat_bowl_dict[key] + 1
    else:
        bat_bowl_dict[key] = 1
    if (
        i[3] == "bowled"
        or i[3] == "caught"
        or i[3] == "lbw"
        or i[3] == "caught and bowled"
        or i[3] == "stumped"
        or i[3] == "hit wicket"
	or i[3] == "obstructing the field"
    ):
        if key in caught_dict:
            caught_dict[key] = caught_dict[key] + 1
        else:
            caught_dict[key] = 1
    else:
        if cmp(i[3], ""):
            if key in caught_dict:
                caught_dict[key] = caught_dict[key]
            else:
                caught_dict[key] = 0


bat_keys = bat_bowl_dict.keys()
for i in bat_keys:
    if bat_bowl_dict[i] > 5:
        final_dict[i] = bat_bowl_dict[i]


final_keys = final_dict.keys()
caught_keys = caught_dict.keys()
list_big = []
for i in final_keys:
    sub_list = []
    if i in caught_keys:
        sub_list.append(i[0])
        sub_list.append(i[1])
        sub_list.append(caught_dict[i])
        sub_list.append(final_dict[i])
        list_big.append(sub_list)

final_sorted_list = Sort(list_big)
for i in final_sorted_list:
    print("%s,%s,%d,%d	" % (i[0], i[1], i[2], i[3]))
