#!/usr/bin/python3
import sys
import operator

result = dict()
for record in sys.stdin.readlines():
    values = record.strip().split(',')
    key, value = ','.join(values[:2]), values[2:4]
    value[0] = int(value[0])
    value[1] = int(value[1])
    if key not in result:
        result[key] = value
    else:
        result[key][0] += value[0]
        result[key][1] += value[1]

modResult = dict()
for elements in result:
    if result[elements][1] > 5:
        modResult[elements] = result[elements]

sorted_list = sorted(modResult.items(), key=operator.itemgetter(1),
                     reverse=True)
start_index = 0
end_index = 0
for i in range(1, len(sorted_list)):
    if(sorted_list[i][1][0] == sorted_list[i-1][1][0]):
        end_index += 1
    elif (end_index-start_index) > 0:
        sorted_list[start_index:end_index+1] =\
            sorted(sorted_list[start_index:end_index+1], key=lambda x: x[1][1])
        start_index = i
        end_index = i
    else:
        start_index = i
        end_index = i


if not(end_index - start_index) > 0:
    value = 'none'
else:
    sorted_list[start_index:end_index+1] =\
        sorted(sorted_list[start_index:end_index+1], key=lambda x: x[1][1])
    start_index = i
    end_index = i

for elements in sorted_list:
    print(elements[0], ',', elements[1][0], ',', elements[1][1], sep='')
