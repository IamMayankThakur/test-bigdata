#!/usr/bin/python3
import sys

d = {}

def func1(x):
 return d[x][0]

def func2(x):
 return d[x][1] 

def func3(x):
 return x[0]

for line in sys.stdin:
 line = line.strip()
 line_val = line.split(',')
 bowl,bat,runs,ball = line_val
 runs = int(runs)
 #print(runs)
 ball = int(ball)
 key = (bowl,bat)
 if key in d:
  d[key][0].append(runs)
  d[key][1].append(ball)
 else:
  d[key] = [[],[]]
  d[key][0].append(runs)
  d[key][1].append(ball)

for key in d.keys(): 
 d[key][0] = sum(d[key][0])
 d[key][1] = sum(d[key][1])

s = sorted(d,key = func3)
s = sorted(s,key = func2)
s = sorted(s,key = func1,reverse = True)
for k in s: 
 if d[k][1] > 5:	
  print('%s,%s,%d,%d' % (k[0],k[1],d[k][0],d[k][1]))
