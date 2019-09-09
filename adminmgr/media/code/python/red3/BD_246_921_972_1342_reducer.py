#!/usr/bin/python3
"""reducer.py"""

from operator import itemgetter
import sys

result = {}
dict_ven = {}
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, value = line.split(':')

    # convert count (currently a string) to int
    # reformatting the input given
    key_attr = key.split('~')
    venue = key_attr[0]
    if(venue not in dict_ven):
        dict_ven.update({venue: 1})
    else:
        dict_ven[venue] = dict_ven[venue]+1
    batsman = key_attr[1]
    key = (venue, batsman)  # in tuple format
    # print(key)

    # reformatting value
    value = value.strip()
    value = value.split(',')
    # print(value)
    ctr = int(value[0][1:])
    runs = int(value[1][:-1])

    if(key in result.keys()):
        result[key][0] += ctr
        result[key][1] += runs

    else:
        result.update({key: [ctr, runs]})
# print(len(result))
# removing all cases where n of deliveries is less than 10
result_temp = {}
reducer_result = {}
for i in result:
    # print(type(i),type(result[i]))
    if(result[i][0] >= 10):
        result_temp.update({i: result[i]})
        # del(result[keys]) # removing elements with less than 10 deliveries
# print(len(result_temp))
for i in result_temp:
    # total runs/ total deliveries
    result_temp[i] = (float(result_temp[i][1])/float(result_temp[i][0]))*100
    #print('%s:%s' % (str(i),str(result_temp[i])))
    if(list(i)[0] in reducer_result):
        reducer_result[list(i)[0]].update({list(i)[1]: result_temp[i]})
    else:
        reducer_result.update({list(i)[0]: {list(i)[1]: result_temp[i]}})

for i in reducer_result:
    res = sorted(reducer_result[i].items(), key=itemgetter(1), reverse=True)
    print(str(i),',', str(list(res[0])[0]))
