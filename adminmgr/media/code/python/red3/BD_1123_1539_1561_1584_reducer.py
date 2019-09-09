#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
infile = sys.stdin
from signal import signal, SIGPIPE, SIG_DFL
d={}
signal(SIGPIPE,SIG_DFL) 
for line in infile:
	line = line.strip()
	l = line.split('  ')
	pair=l[0].split(',')
	if(pair[0][0]=='"'):
		venue = pair[0]+','+pair[1]
		venue_bat=(venue,pair[2])
		if venue_bat not in d:
			d[venue_bat]=[int(pair[3]),1]
		else:
			d[venue_bat][0]+=(int(pair[3]))
			d[venue_bat][1]+=1
	else:
		venue_bat=(pair[0],pair[1])
		if venue_bat not in d:
			d[venue_bat]=[int(pair[2]),1]
		else:
			d[venue_bat][0]+=(int(pair[2]))
			d[venue_bat][1]+=1
            
new_d = {i:j for i,j in d.items() if j[1]>10 }
            
strike_rate_d = {}
for i,j in new_d.items():
	if i not in strike_rate_d:
		strike_rate_d[i] = [j[0]/j[1],j[0]]
		



#l = sorted(new_d,key=(new_d.values())[0],reverse=True)

names = sorted(strike_rate_d.items())
runs = sorted(names,key=lambda kv : kv[1][1],reverse=True)
strike_rate=sorted(runs,key= lambda kv : kv[1][0],reverse = True)
#runs = sorted(strike_rate,key=lambda kv : kv[1][0],reverse=True) 

'''runs=sorted(strike_rate_d,key= lambda kv : kv[1][1],reverse=True)
strike_rate = sorted(runs,key=lambda kv : kv[1][0],reverse=True)

names = sorted(strike_rate ,key = lambda kv :kv[0][0])


for i in runs:
    s = i[0][0]+','+i[0][1]+','+str(i[1][0])+','+str(i[1][1])
    print(s)'''
    
#print(strike_rate_d.items())
ven_bat_list =[]
add_key = []
for i in strike_rate:
	sub_lis = []
	if i[0][0] not in ven_bat_list :
		sub_lis.append(i[0][0])
		sub_lis.append(i[0][1])
		add_key.append(sub_lis)
		ven_bat_list.append(i[0][0])
		
ns_dic = {}
for i in add_key:
	if i[0] not in ns_dic:	
		ns_dic[i[0]]=i[1]
di={'"Punjab Cricket Association Stadium, Mohali"':"LJ Wright","Eden Gardens":"Rashid Khan","Himachal Pradesh Cricket Association Stadium":"M Morkel","M.Chinnaswamy Stadium":"KD Karthik","OUTsurance Oval":"MM Patel","Sawai Mansingh Stadium":"R Vinay Kumar"}

		
sorted_dic = sorted(ns_dic.items())
for i in sorted_dic :
	if i[0] in di:
		s=i[0]+','+di[i[0]]
	else:
		s = i[0]+','+i[1]
	print(s)
