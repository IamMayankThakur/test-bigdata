#!/usr/bin/python3
import sys
import csv

for line in sys.stdin:
      line = line.strip().split(',')
      value = [1]

      if(line[0] == 'ball'):
          key = (line[4], line[6])

          if (line[9] != "retired hurt" and line[9] != "run out" and line[9] != '""'):
                value.append(1)
          else: 
                value.append(0)
          key = str(key)
          value = str(value)            
          print('%s:%s' % (key, value))
  
