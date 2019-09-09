#!/usr/bin/python
import sys
infile = sys.stdin

for line in infile:
 line = line.strip()
 my_list = line.split(',')
 if my_list[0] != "info" and my_list[0] != "version":
  bowler = my_list[6]
  batsman = my_list[4]
  runs = my_list[7]
  extras = my_list[8]
  run = int(runs)
  #print(run)
  extra = int(extras)
  print('%s,%s,%d,%d' % (bowler,batsman,run+extra,1))
