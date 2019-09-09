#!/usr/bin/python

import sys
import csv
infile = sys.stdin
#next(infile)
venue = ''
for line in infile:
   line = line.strip()
   info = line.split(',')
   if(info[1] == 'venue'):
      try:
         venue=info[2]+","+info[3]
      except:
         venue=info[2] #if venue does not contain a comma
   elif(info[0] == "ball"):
         if(int(info[8])==0):
            batrun= info[4]+','+info[7] #runs scored by the batsman in the ball held together in a single variable
            print('%s\t%s' % (venue,batrun))
