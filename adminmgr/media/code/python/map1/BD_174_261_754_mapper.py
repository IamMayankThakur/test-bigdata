#!/usr/bin/python3
import sys

for line in sys.stdin:
    line = line.strip()
    data = line.split(',')
    mapper_list = []
    if(len(data) > 6):
        if(len(data) > 8):
            if(data[9] == '\"\"'):
                mapper_list.append([data[4], data[6], data[0]])
            else:
                mapper_list.append([data[4], data[6], data[9]])
    for i in mapper_list:
        print("%s||%s--%s" % (i[0], i[1], i[2]))
