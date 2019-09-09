#!/usr/bin/python3
import sys
import csv

for line in sys.stdin:
      value = [1]
      line = line.strip().split(',')
  
      if(line[0] == 'ball'):
          key = (line[6], line[4])
          key = str(key)
          x = int(line[7])
          y = int(line[8])
          z = x + y
          
          value.append(z)
          
          value = str(value)            
          print('%s:%s' % (key, value))
  
