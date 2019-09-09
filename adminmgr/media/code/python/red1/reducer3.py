#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
my_dict = dict()
current_bowler = ""
current_batsman = ""
current_run = 0
current_deli = 0
for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	bowler,batsman,run,deli = line_val[0], line_val[1], int(line_val[2]), int(line_val[3])
	try:
		count_run  = run
		count_deli = deli
		
	
	
	except ValueError:
		continue
	if(current_bowler == bowler and current_batsman == batsman):
		current_run += count_run
		current_deli += count_deli
	else:
		
		if (current_batsman != "" ):
			print('%s\t%s\t%d\t%d'% (current_bowler,current_batsman,current_run,current_deli))
		current_run = run
		current_deli = deli
		current_bowler = bowler
		current_batsman = batsman
		
if(current_bowler == bowler and current_batsman == batsman and current_deli > 5):
	print('%s\t%s\t%d\t%d'% (current_bowler,current_batsman,current_run,current_deli))


"""
	if((bowler,batsman) in my_dict.keys()):
        	a = my_dict[bowler,batsman]
        	my_dict[bowler,batsman] = (run + a[0] , deli+a[1]) 
    	else:
        	my_dict[bowler,batsman] = (run , deli)
		
	


for k,v in list(my_dict.items()):
    v1,v2 = v[0],v[1]
    #print(v1,v2)
    if(v2 < 10):
        del my_dict[k]

for q,w in list(my_dict.items()):
    sr=float(w[0]/w[1])
    for r,t in list(my_dict.items()):
        if(q[0]==r[0] and (float(t[0]/t[1])<sr)):
           del my_dict[r]
        elif(q[0]==r[0] and (float(t[0]/t[1])==sr)):
           if(t[1]>w[1]):
             del my_dict[q] 
	   else:
             del my_dict[q]

initial=[]
initial=[(o,p) for o,p in my_dict.items()]
final=[]
final=sorted(initial,reverse=True,key = lambda x:x[0][0])
print(final)





	if(current_bowler == bowler and current_batsman == batsman):
		
		current_run += count_run
		current_deli += count_deli
	else:
		if (current_batsman != "" and current_deli > 5):
			print('%s\t%s\t%d\t%d'% (current_bowler,current_batsman,current_run,current_deli))
		current_run = count_run
		current_deli = count_deli
		current_bowler = bowler
		current_batsman = batsman
		
if(current_bowler == bowler and current_batsman == batsman and current_deli > 5):
	print('%s\t%s\t%d\t%d'% (current_bowler,current_batsman,current_run,current_deli))
"""
