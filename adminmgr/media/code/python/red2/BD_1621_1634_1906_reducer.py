#!/usr/bin/python3
import csv
from operator import itemgetter
import sys




global current_key
global ball_count
global no_of_wik
current_key = ""
no_of_wik=0
current_count=1



lis=[]



for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	key, val = line_val[0], line_val[1]
	try:
		wik = int(val)
	except ValueError:
		continue
    
	
	if current_key == key:
		current_count += 1
		no_of_wik+=wik
		
		
		
		
	else:
		if (current_key != ""):
			
			if(current_count>5):
        		#print '%s' % (current_key+','+str(current_count)+','+str(ball_count))
				p=current_key
				t=dict()
				a,b=p.split(",")
				t["bat"]=a
				t["bow"]=b
				t["wick"]=no_of_wik*-1
				t["deli"]=current_count
				lis.append(t)
			
		current_count = 1
		no_of_wik=int(val)
		current_key = key
    
if current_key == key:
	if(current_count>5):
    		#print '%s' % (current_key+','+str(current_count)+','+str(ball_count))
		p=current_key
		a,b=p.split(",")
		t=dict()
		t["bat"]=a
		t["bow"]=b
		t["wick"]=no_of_wik*-1
		t["deli"]=current_count
		lis.append(t)
lis=sorted(lis, key=itemgetter('wick','deli','bow')) 

for a in lis:	
	print ('%s,%s,%s,%s'%(a["bow"],a["bat"],int(a["wick"])*-1,a["deli"]))


	


