#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import itertools
current_count = 0
current_key = ""
current_count = 0
current_key = ""
lana = dict()
i = 0
for line in sys.stdin:
    line = line.strip()
    line_val = line.split(",")
    var_venue,var_name,var_runs,var_balls = line_val[0], line_val[1],int(line_val[2]),int(line_val[3])
	
    if((var_venue,var_name) in lana.keys()):
        d = lana[var_venue,var_name]
        lana[var_venue,var_name] = (var_runs + d[0] , var_balls+d[1]) 
    else:
        lana[var_venue,var_name] = (var_runs,var_balls)
		
for k,v in list(lana.items()):
    v1,v2 = v[0],v[1]
    #print(v1,v2)
    if(v2 < 10):
        del lana[k]

for q,w in list(lana.items()):
    sr=float(w[0]/w[1])
    for r,t in list(lana.items()):
        if(q[0]==r[0] and (float(t[0]/t[1])<sr)):
           del lana[r]
        elif(q[0]==r[0] and (float(t[0]/t[1])==sr)):
           if(t[1]>w[1]):
             del lana[q] 
	   else:
             del lana[q]

initial=[]
initial=[(o,p) for o,p in lana.items()]
final=[]
final=sorted(initial,reverse=True,key = lambda x:x[0][0])
print(final)
			
			
			 
