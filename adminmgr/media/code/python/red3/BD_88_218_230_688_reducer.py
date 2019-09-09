#!/usr/bin/python3
#Reducer.py
import sys

dict_ven={}

#Partitoner
for line in sys.stdin:
    line=line.strip('\n')
    if (len(line.split(','))==4):
        venue,v1,batsman,run=line.split(',')
        venue=venue+','+v1
    else:
        venue,batsman,run=line.split(',')
    key=(venue,batsman)
    if(key not in dict_ven):
      dict_ven[key]=[]
      dict_ven[key].append(int(run))
    else:
      dict_ven[key].append(int(run))
#print(dict_ven)
ven=dict_ven.keys()
ven=[i[0] for i in ven]
ven=sorted(set(ven))
#print(venue)
ven_sr={}
for key in dict_ven:
    value=dict_ven[key]
    venue=key[0]
    batsman=key[1]
    total_run=sum(value)
    deliveries=len(value)
    strike_rate=total_run/deliveries
    dict_ven[key]=[strike_rate,deliveries]
    if(deliveries>=10):
      if(venue not in ven_sr):
        ven_sr[venue]=[]
        ven_sr[venue].append([strike_rate,total_run,batsman])
      else:
        ven_sr[venue].append([strike_rate,total_run,batsman])
for key in ven_sr:
    ven_sr[key]=sorted(ven_sr[key],key=lambda x:(x[0],x[1]),reverse=True)
#    print(key,ven_sr[key],'\n\n')
for i in ven:
	print(i,',',ven_sr[i][0][2],sep='')


