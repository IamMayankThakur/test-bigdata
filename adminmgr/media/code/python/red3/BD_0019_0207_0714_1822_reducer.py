#!/usr/bin/python3
import sys
import itertools

current_count = 0
current_key = ""
current_count = 0
current_key = ""
final_dict = dict()
i = 0
final = dict()
runs = dict()
runs_1=dict()
for line in sys.stdin: 
    line = line.strip() 
    line_val = line.split(",") 
    key1,key2,key3,key4 = line_val[0], line_val[1],int(line_val[2]),int(line_val[3])

    if((key1,key2) in final_dict.keys()):
        d = final_dict[key1,key2]
	
        final_dict[key1,key2] = (key3 + d[0] , key4 + d[1]) 
    else:
        final_dict[key1,key2] = (key3,key4)

old = []
for i in sorted(final_dict.items(),reverse = True,key = lambda x:x[1]) :
    old.append(list(i))
    runs_1.update(i)
i = 0
j = 0

some=""


ano=[]
rr=dict()
for i in range(len(old)):
	if(old[i][0][0] not in ano):
	    some=old[i][0][0]
	    ano.append(old[i][0][0])
	    rr[old[i][0][0],old[i][0][1]]=(old[i][1][0],old[i][1][1])
for k,v in sorted(rr.items(),reverse = False,key = lambda x:x[0]):
	ru=int(v[0])
	ba=int(v[1])
	rrn=(ru/float(ba))*100
	print(k[0]+","+k[1])
    
  
   

