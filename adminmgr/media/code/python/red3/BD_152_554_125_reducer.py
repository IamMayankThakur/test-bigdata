#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

current_venue = ""
current_batsman = ""
runs = 0
balls = 0
sr = 0
l=[]

for line in sys.stdin:
  line = line.strip()
  line_val = line.split(',')
  if(len(line_val) == 4):
    venue, batsman = line_val[0], line_val[1]
    run = int(line_val[2])
    ball = int(line_val[3])
  else:
    venue = line_val[0] + "," + line_val[1]
    batsman = line_val[2]
    run = int(line_val[3])
    ball = int(line_val[4])
  if current_venue == venue:
    if current_batsman == batsman:
      runs += run
      balls += ball
    else:
      if (current_batsman != ""):
        if (balls >= 10):
          sr = (runs/balls)*100
          l.append([current_batsman,runs,balls,sr])
      current_batsman = batsman
      runs = run
      balls = ball
  else:
    if (current_venue != ""):
      if(balls >= 10):
        sr = (runs/balls)*100
        l.append([current_batsman,runs,balls,sr])
      l.sort(key=lambda x: (-x[3],x[1],x[0]))
      print('%s,%s' % (current_venue, l[0][0]))
      l=[]
      runs = run
      balls = ball
      current_batsman = batsman
      current_venue = venue
    else:
      current_venue = venue
      current_batsman = batsman
      runs = run
      balls = ball

if (current_batsman == batsman) and (current_venue == venue):
  if (balls >= 10):
    sr = (runs/balls)*100
    l.append([current_batsman,runs,balls,sr])
    l.sort(key=lambda x: (-x[3],x[1],x[0]))
    print('%s,%s' % (current_venue,l[0][0]))
