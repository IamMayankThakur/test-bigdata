#!/usr/bin/python
import sys

dic = {}

def func1(x):
 return dic[x][0]

def func2(x):
 return dic[x][1] 

def func3(x):
 return x[0]

for line in sys.stdin:
 line = line.strip()
 line_val = line.split(',')
 bowler,batsman,runs,balls = line_val
 runs = int(runs)
 #print(runs)
 balls = int(balls)
 key = (bowler,batsman)
 if key in dic:
  dic[key][0].append(runs)
  dic[key][1].append(balls)
 else:
  dic[key] = [[],[]]
  dic[key][0].append(runs)
  dic[key][1].append(balls)

for key in dic.keys(): 
 dic[key][0] = sum(dic[key][0])
 dic[key][1] = sum(dic[key][1])

s = sorted(dic,key = func3)
s = sorted(s,key = func2)
s = sorted(s,key = func1,reverse = True)
for k in s: 
 if dic[k][1] > 5:	
  print('%s,%s,%d,%d' % (k[0],k[1],dic[k][0],dic[k][1]))
