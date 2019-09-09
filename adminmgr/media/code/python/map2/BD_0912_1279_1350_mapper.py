#!/usr/bin/python
import sys
infile = sys.stdin

for o in infile:
 o = o.strip()
 m_l = o.split(',')
 if m_l[0] != "info" and m_l[0] != "version":
  bowler = m_l[6]
  batsman = m_l[4]
  runs = m_l[7]
  extras = m_l[8]
  run = int(runs)
  #print(run)
  extra = int(extras)
  print('%s,%s,%d,%d' % (bowler,batsman,run+extra,1))
