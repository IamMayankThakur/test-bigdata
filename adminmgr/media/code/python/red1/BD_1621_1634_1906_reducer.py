#!/usr/bin/python3
import csv
from operator import itemgetter
import sys




global current_key
global current_count
global ball_count
current_key = ""
current_count = 0
ball_count=1


lis=[]



for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	key, val = line_val[0], line_val[1]
	try:
		count = int(val)
	except ValueError:
		continue
    
	
	if current_key == key:
		current_count += count
		ball_count=ball_count+1
		
		
	else:
		if (current_key != ""):
			
			if(ball_count>5):
        		#print '%s' % (current_key+','+str(current_count)+','+str(ball_count))
				p=current_key
				t=dict()
				a,b=p.split(",")
				t["bat"]=a
				t["bow"]=b
				t["wick"]=current_count*-1
				t["deli"]=ball_count
				lis.append(t)
			
		current_count = count
		ball_count=1
		current_key = key
    
if current_key == key:
	if(ball_count>5):
    		#print '%s' % (current_key+','+str(current_count)+','+str(ball_count))
		p=current_key
		a,b=p.split(",")
		t=dict()
		t["bat"]=a
		t["bow"]=b
		t["wick"]=current_count*-1
		t["deli"]=ball_count
		lis.append(t)
lis=sorted(lis, key=itemgetter('wick','deli','bat')) 

for a in lis:	
	print ('%s,%s,%s,%s'%(a["bat"],a["bow"],int(a["wick"])*-1,a["deli"]))


	


