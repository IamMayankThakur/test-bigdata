#!/usr/bin/python3
import sys
infile = sys.stdin

for line in infile:
 line = line.strip()
 my_list = line.split(',')
 if my_list[0] != "info" and my_list[0] != "version":
  bowl = my_list[6]
  bat = my_list[4]
  runs = my_list[7]
  extras = my_list[8]
  run = int(runs)
  #print(run)
  extra = int(extras)
  print('%s,%s,%d,%d' % (bowl,bat,run+extra,1))
