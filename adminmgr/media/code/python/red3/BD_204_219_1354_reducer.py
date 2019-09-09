#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
from operator import itemgetter
import sys

res = {}
run = 0
for line in sys.stdin:
    line = line.strip()
    line_val = line.split('\t')
    #print(line_val)
    key, val = line_val[0], line_val[1]
    key_list = key.split(';')
    '''
    if len(key_list) == 4:
        pvpair = key_list[0] + ',' + key_list[1] + ',' + key_list[2]
        run = key_list[3]
    else:
    '''
    pvpair = key_list[0] + ';' + key_list[1]
    run = key_list[2]
    if pvpair in res.keys():
        res[pvpair][0] = res[pvpair][0] + int(run)
        res[pvpair][1] = 1 + res[pvpair][1]
    else:

        res[pvpair] = [int(run), 1, 0]
res1 = {}

for x in res.keys():
    res[x][2] = float(res[x][0]) * 100 / res[x][1]
    res[x][2] = float('{0:.2f}'.format(res[x][2]))
    if res[x][1] >= 10:
        res1[x] = res[x]

# print(res)

# print(res1)

res_run = sorted(res1.items(), key=lambda x: x[1][0], reverse=True)
res_sr = sorted(res_run, key=lambda x: x[1][2], reverse=True)
res_key = sorted(res_sr, key=lambda x: x[0].split(';')[0])

# print(res_key)

final = {}

for x in res_key:
    a = x[0].split(';')
    if a[0] not in final:
        final[a[0]] = a[1]

final = sorted(final.items())

for x in final:
    print(x[0] + ',' + x[1])

