#!/usr/bin/python
import sys
d = {}
def myfun1(x):
 return d[x][0]
def myfun2(x):
 return d[x][1] 
def myfun3(x):
 return x[0]
for o in sys.stdin:
 o = o.strip()
 line_val = o.split(',')
 bowler,batsman,runs,balls = line_val
 runs = int(runs)
 balls = int(balls)
 key = (bowler,batsman)
 if key in d:
  d[key][0].append(runs)
  d[key][1].append(balls)
 else:
  d[key] = [[],[]]
  d[key][0].append(runs)
  d[key][1].append(balls)
for key in d.keys(): 
 d[key][0] = sum(d[key][0])
 d[key][1] = sum(d[key][1])
y = sorted(d,key = myfun3)
y = sorted(y,key = myfun2)
y = sorted(y,key = myfun1,reverse = True)
for k in y: 
 if d[k][1] > 5:	
  print('%s,%s,%d,%d' % (k[0],k[1],d[k][0],d[k][1]))
